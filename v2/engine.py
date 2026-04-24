"""
OTU Wheel v2.0 — Unified Engine

Three job types, one orchestration loop:
  • run_entry_csp()   — Morning Brief: CORE_WHEEL + SAFE_HAVEN, Kelly-ranked
  • run_entry_leap()  — LEAP alerts: all 40, T1/T2 individual messages
  • run_manage()      — MANAGE: scan open positions, dispatch triggers

Each job:
  1. Loads macro events if stale
  2. Reads live VIX (for adaptive thresholds)
  3. Scans its ticker set
  4. Applies hard filters → conviction score → tier classification
  5. Sends Discord output + logs to alerts_log for dedupe
"""

from __future__ import annotations

import time
import datetime as _dt
from typing import Optional

import requests
import pytz

from . import (
    db, iv_rank, macro_calendar, scoring, filters, kelly,
    fundamentals, schwab_client, discord_output, universe,
    manage_module, entry_cc,
)


def run_entry_cc(schwab_headers: dict, webhook_url: str,
                 target_expiry: Optional[str] = None) -> int:
    return entry_cc.run_entry_cc(schwab_headers, webhook_url, target_expiry)


ET = pytz.timezone("America/New_York")
TARGET_EXPIRY = "2026-05-15"   # fallback only — replaced by get_target_expiry() at runtime


def get_target_expiry(min_dte: int = 28, max_dte: int = 35) -> str:
    """
    Returns the nearest Friday (weekly or monthly) in the [28, 35] DTE window
    — approximately 4-5 weeks out. This is the user's preferred horizon for
    the wheel: close enough for fast theta decay, far enough to collect
    meaningful premium.

    Strategy: scan every Friday for the next 60 days:
      1. Prefer a Friday in [min_dte, max_dte] — pick the one nearest the
         centre of the window (best balance of premium vs. time).
      2. Fallback: nearest Friday >= min_dte if nothing lands in range
         (e.g., right after a monthly expiry).

    OI / spread filters in the main scan will naturally eliminate any weekly
    strikes that are illiquid for a given ticker. Liquid names (PLTR, AMD,
    NVDA, etc.) have solid weekly OI; illiquid ones get flagged and appear
    in the near-miss list.
    """
    today = _dt.date.today()

    fridays: list[tuple[int, _dt.date]] = []
    for offset in range(1, 61):
        d = today + _dt.timedelta(days=offset)
        if d.weekday() == 4:          # Friday
            fridays.append((offset, d))

    # 1. All Fridays in [min_dte, max_dte]
    in_window = [(dte, d) for dte, d in fridays if min_dte <= dte <= max_dte]
    if in_window:
        mid = (min_dte + max_dte) / 2
        return min(in_window, key=lambda x: abs(x[0] - mid))[1].isoformat()

    # 2. Nearest Friday >= min_dte
    above = [(dte, d) for dte, d in fridays if dte >= min_dte]
    if above:
        return min(above, key=lambda x: x[0])[1].isoformat()

    return TARGET_EXPIRY  # last resort


# ── Macro (VIX from Yahoo, SPY/EMA200 from Schwab) ───────────────────────────

def _get_vix() -> Optional[float]:
    """Fresh VIX from Yahoo with crumb handshake."""
    try:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        s.get("https://finance.yahoo.com", timeout=10)
        crumb = s.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10).text.strip()
        j = s.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1d&range=1d&crumb={crumb}",
            timeout=10,
        ).json()
        return float(j["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception as e:
        print(f"  [VIX] error: {e}")
        return None


def _get_macro(schwab_headers: dict) -> dict:
    vix = _get_vix()
    spy_candles = schwab_client.get_daily_candles(schwab_headers, "SPY", years=2, inject_live_close=True)
    spy = spy_candles[-1]["close"] if spy_candles else None

    # EMA200 on SPY closes
    closes = [c["close"] for c in spy_candles]
    ema200 = None
    if len(closes) >= 200:
        k = 2 / 201
        ema = sum(closes[:200]) / 200
        for p in closes[200:]:
            ema = p * k + ema * (1 - k)
        ema200 = round(ema, 2)

    vix_rule = _vix_rule(vix)
    return {
        "vix":      round(vix, 2) if vix is not None else None,
        "spy":      round(spy, 2) if spy else None,
        "ema200":   ema200,
        "bear_market": bool(spy and ema200 and spy < ema200),
        "vix_rule": vix_rule,
    }


def _vix_rule(vix: Optional[float]) -> str:
    if vix is None:                  return "VIX unavailable"
    if vix < 10:                     return "75-100% cash (VIX <10)"
    if vix < 15:                     return "50-75% cash (VIX 10-15)"
    if vix < 20:                     return "25-50% cash (VIX 15-20)"
    if vix < 30:                     return "0% cash — fully deployed (VIX 20-30)"
    return "ADD NEW CASH DEPOSITS (VIX 30+)"


# ── Shared candidate builder ─────────────────────────────────────────────────

def _evaluate_candidate(schwab_headers: dict, ticker: str,
                        vix: Optional[float], target_expiry: str) -> Optional[dict]:
    """
    Runs the full pipeline for one ticker. Returns a dict with everything the
    caller needs to decide (tier, score, filters, ROI, Kelly). Returns None on
    any unrecoverable error.
    """
    candles = schwab_client.get_daily_candles(schwab_headers, ticker)
    if len(candles) < 50:
        return None
    closes = [c["close"] for c in candles]
    price  = closes[-1]

    # IV Rank (cached)
    ivr = iv_rank.compute_iv_rank(schwab_headers, ticker)

    # Option leg near target expiry + 25 delta (CSP window 0.20-0.30)
    opt = schwab_client.get_put_chain_near_delta(schwab_headers, ticker,
                                                  target_expiry, target_delta=0.25)
    if opt is None:
        return None

    # Fundamentals + earnings
    fund = fundamentals.get_fundamentals(ticker)

    # Spread percent of mid
    spread_pct = None
    if opt["mid"] > 0 and opt["ask"] > 0:
        spread_pct = (opt["ask"] - opt["bid"]) / opt["mid"] * 100.0

    # Hard filters
    passed, flags = filters.passes_hard_filters(
        iv_rank=ivr,
        open_interest=opt["open_interest"],
        bid=opt["bid"],
        ask=opt["ask"],
        strike=opt["strike"],
        price=price,
        expiry=opt["expiry"],
        earnings_date=fund.get("earnings_date"),
        closes=closes,
    )

    # Conviction score
    inp = scoring.ConvictionInputs(
        price=price, closes=closes, candles=candles,
        iv_rank=ivr, pe_positive=fund["pe_positive"], beats_4q=fund["beats_4q"],
        open_interest=opt["open_interest"], spread_pct_of_mid=spread_pct,
    )
    base_score, details = scoring.calc_conviction(inp)
    score = scoring.apply_vix_modifier(base_score, vix)
    tier, tier_desc = scoring.classify_tier(score, vix)

    # Kelly
    wr = details.get("backtest_wr", 50)
    kelly_s = kelly.kelly_score(opt["roi"], wr, max_loss_pct=15.0)

    dte = (_dt.date.fromisoformat(opt["expiry"]) - _dt.date.today()).days

    return {
        "ticker":       ticker,
        "price":        round(price, 2),
        "strike":       opt["strike"],
        "delta":        opt["delta"],
        "mid":          opt["mid"],
        "bid":          opt["bid"],
        "ask":          opt["ask"],
        "iv":           opt["iv"],
        "iv_rank":      ivr,
        "roi":          opt["roi"],
        "kelly":        kelly_s,
        "dte":          dte,
        "expiry":       opt["expiry"],
        "open_interest": opt["open_interest"],
        "spread_pct":   spread_pct,
        "score":        score,
        "base_score":   base_score,
        "tier":         tier,
        "tier_desc":    tier_desc,
        "details":      details,
        "flags":        flags,
        "passed":       passed,
        "backtest_wr":  wr,
        "pe_positive":  fund["pe_positive"],
        "beats_4q":     fund["beats_4q"],
        "earnings_date": fund.get("earnings_date"),
    }


# ── Job 1: ENTRY-CSP (Morning Brief) ─────────────────────────────────────────

def run_entry_csp(schwab_headers: dict, webhook_url: str, slot_label: str,
                   target_expiry: Optional[str] = None) -> int:
    target_expiry = target_expiry or get_target_expiry()
    print(f"\n{'='*60}\n[ENTRY-CSP] Morning Brief — {slot_label} ET | expiry={target_expiry}")

    # Refresh macro calendar if stale (weekly)
    if macro_calendar.macro_is_stale(max_age_days=7):
        n = macro_calendar.refresh_macro_calendar()
        print(f"  [MACRO] refreshed ({n} events)")

    macro = _get_macro(schwab_headers)
    vix   = macro["vix"]
    print(f"  VIX={vix} SPY=${macro['spy']} EMA200=${macro['ema200']}")

    qualified: list[dict] = []
    near_miss: list[dict] = []   # evaluated but blocked — useful when 0 qualify
    scanned = 0
    for ticker in universe.CSP_SCAN:
        scanned += 1
        try:
            time.sleep(0.35)
            c = _evaluate_candidate(schwab_headers, ticker, vix, target_expiry)
            if c is None:
                print(f"  {ticker}: no data")
                continue
            # Track why it failed (for near-miss list)
            if not c["passed"]:
                c["reject_reason"] = "filters: " + (",".join(c["flags"]) or "-")
                near_miss.append(c)
                print(f"  {ticker}: filtered ({','.join(c['flags'])})")
                continue
            if c["roi"] < 3.0:
                c["reject_reason"] = f"ROI_LOW({c['roi']}%<3%)"
                near_miss.append(c)
                print(f"  {ticker}: ROI too low ({c['roi']}%)")
                continue
            # Kelly <= 0 is informational only — still qualify, just rank lower
            qualified.append(c)
            print(f"  {ticker}: OK roi={c['roi']}% kelly={c['kelly']} ivr={c['iv_rank']}")
        except Exception as e:
            print(f"  {ticker}: error {e}")

    # Sort by Kelly desc, then score as tiebreaker (kelly=0 candidates go last)
    qualified.sort(key=lambda r: (r["kelly"], r["score"]), reverse=True)

    # Sort near-misses by score desc (so top 5 are most interesting)
    near_miss.sort(key=lambda r: r.get("score", 0), reverse=True)

    # Build + send
    events = macro_calendar.upcoming_events(days_ahead=5)
    msg = discord_output.morning_brief_message(
        slot_label=slot_label, macro=macro, qualified=qualified,
        upcoming_events=events, total_scanned=scanned,
        target_expiry=target_expiry, vix_rule=macro["vix_rule"],
        near_miss=near_miss[:8],
    )
    discord_output.send(webhook_url, msg)

    # Log each qualifying ticker to alerts_log with full contract snapshot
    # so the outcome evaluator (Camino B) can measure real performance later.
    for r in qualified:
        db.log_alert(
            r["ticker"], "ENTRY-CSP",
            tier=r["tier"], score=r["score"],
            side="PUT",
            strike=r.get("strike"), expiry=r.get("expiry"),
            mid_at_alert=r.get("mid"), delta_at_alert=r.get("delta"),
            iv_rank_at_alert=r.get("iv_rank"), roi_at_alert=r.get("roi"),
            price_at_alert=r.get("price"),
        )

    print(f"[ENTRY-CSP] Done — {len(qualified)} qualifying / {scanned} scanned")
    return len(qualified)


# ── Job 2: ENTRY-LEAP ────────────────────────────────────────────────────────

def run_entry_leap(schwab_headers: dict, webhook_url: str) -> int:
    target_expiry = get_target_expiry()
    print(f"\n{'='*60}\n[ENTRY-LEAP] Trade Alerts scan | expiry={target_expiry}")

    if macro_calendar.macro_is_stale(max_age_days=7):
        macro_calendar.refresh_macro_calendar()

    vix = _get_vix()
    t1_th, t2_th = scoring.tier_thresholds(vix)
    print(f"  VIX={vix} | thresholds T1>={t1_th} T2>={t2_th}")

    candidates: list[dict] = []
    near_miss: list[dict] = []
    scanned = 0
    for ticker in universe.LEAP_SCAN:
        scanned += 1
        try:
            time.sleep(0.35)
            c = _evaluate_candidate(schwab_headers, ticker, vix, target_expiry)
            if c is None:
                continue
            if not c["passed"]:
                c["reject_reason"] = "filters: " + (",".join(c["flags"]) or "-")
                near_miss.append(c); continue
            if c["tier"] is None:
                c["reject_reason"] = f"score {c['score']} < T2 ({t2_th})"
                near_miss.append(c); continue

            # Dedupe vs DB: skip if already alerted same-or-better tier in 24h
            prev_tier = db.last_alert_tier(ticker, "ENTRY-LEAP", hours=24)
            if prev_tier is not None and prev_tier <= c["tier"]:
                print(f"  {ticker}: dup (prev T{prev_tier})")
                continue
            c["prev_tier"] = prev_tier
            candidates.append(c)
        except Exception as e:
            print(f"  {ticker}: error {e}")

    # Sort by score desc (T1 before T2)
    candidates.sort(key=lambda r: r["score"], reverse=True)

    t1_count = sum(1 for c in candidates if c["tier"] == 1)
    t2_count = sum(1 for c in candidates if c["tier"] == 2)

    sent = 0
    for c in candidates:
        msg = discord_output.leap_alert_message(
            ticker=c["ticker"], score=c["score"], tier=c["tier"],
            tier_desc=c["tier_desc"], d=c["details"],
            iv_rank=c["iv_rank"], kelly=c["kelly"],
            prev_tier=c.get("prev_tier"),
        )
        if discord_output.send(webhook_url, msg):
            d = c.get("details", {}) or {}
            db.log_alert(
                c["ticker"], "ENTRY-LEAP",
                tier=c["tier"], score=c["score"],
                side="CALL",
                strike=c.get("strike") or d.get("strike"),
                expiry=c.get("expiry") or d.get("expiry"),
                mid_at_alert=c.get("mid") or d.get("mid"),
                delta_at_alert=c.get("delta") or d.get("delta"),
                iv_rank_at_alert=c.get("iv_rank"),
                roi_at_alert=c.get("roi") or d.get("roi"),
                price_at_alert=c.get("price") or d.get("price"),
            )
            sent += 1
            time.sleep(0.5)

    # Always send scan summary + top near-misses so the user sees what's close
    near_miss.sort(key=lambda r: r.get("score", 0), reverse=True)
    summary = discord_output.leap_summary_with_near_miss(
        scanned=scanned, sent=sent, t1_count=t1_count, t2_count=t2_count,
        t1_th=t1_th, t2_th=t2_th, vix=vix, near_miss=near_miss[:8],
    )
    discord_output.send(webhook_url, summary)

    print(f"[ENTRY-LEAP] Done — {sent} sent / {scanned} scanned (T1={t1_count} T2={t2_count})")
    return sent


# ── Job 3: MANAGE ────────────────────────────────────────────────────────────

def run_manage(schwab_headers: dict, webhook_url: str) -> int:
    print(f"\n{'='*60}\n[MANAGE] Position scan")
    alerts = manage_module.scan_open_positions(schwab_headers)

    sent = 0
    for a in alerts:
        msg = discord_output.manage_message(a)
        if discord_output.send(webhook_url, msg):
            db.log_alert(a.ticker, "MANAGE", subtype=a.subtype)
            sent += 1
            time.sleep(0.3)

    # Always notify the heartbeat even if 0
    summary = discord_output.scan_summary_message(
        "MANAGE", scanned=len(alerts), sent=sent,
        extras="no triggers" if sent == 0 else None
    )
    discord_output.send(webhook_url, summary)

    print(f"[MANAGE] Done — {sent} sent / {len(alerts)} triggered")
    return sent
