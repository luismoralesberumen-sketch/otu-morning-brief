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
    manage_module,
)


ET = pytz.timezone("America/New_York")
TARGET_EXPIRY = "2026-05-15"   # overridable via env in main.py


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

    # Option leg near target expiry + 30 delta
    opt = schwab_client.get_put_chain_near_delta(schwab_headers, ticker,
                                                  target_expiry, target_delta=0.30)
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
                   target_expiry: str = TARGET_EXPIRY) -> int:
    print(f"\n{'='*60}\n[ENTRY-CSP] Morning Brief — {slot_label} ET")

    # Refresh macro calendar if stale (weekly)
    if macro_calendar.macro_is_stale(max_age_days=7):
        n = macro_calendar.refresh_macro_calendar()
        print(f"  [MACRO] refreshed ({n} events)")

    macro = _get_macro(schwab_headers)
    vix   = macro["vix"]
    print(f"  VIX={vix} SPY=${macro['spy']} EMA200=${macro['ema200']}")

    qualified: list[dict] = []
    scanned = 0
    for ticker in universe.CSP_SCAN:
        scanned += 1
        try:
            time.sleep(0.35)
            c = _evaluate_candidate(schwab_headers, ticker, vix, target_expiry)
            if c is None:
                print(f"  {ticker}: no data")
                continue
            # For the CSP brief we want: passes filters AND kelly > 0 AND roi >= 3%
            if not c["passed"]:
                print(f"  {ticker}: filtered ({','.join(c['flags'])})")
                continue
            if c["kelly"] <= 0 or c["roi"] < 3.0:
                print(f"  {ticker}: low edge (roi={c['roi']}%, kelly={c['kelly']})")
                continue
            qualified.append(c)
            print(f"  {ticker}: OK roi={c['roi']}% kelly={c['kelly']} ivr={c['iv_rank']}")
        except Exception as e:
            print(f"  {ticker}: error {e}")

    # Sort by Kelly desc
    qualified.sort(key=lambda r: r["kelly"], reverse=True)

    # Build + send
    events = macro_calendar.upcoming_events(days_ahead=5)
    msg = discord_output.morning_brief_message(
        slot_label=slot_label, macro=macro, qualified=qualified,
        upcoming_events=events, total_scanned=scanned,
        target_expiry=target_expiry, vix_rule=macro["vix_rule"],
    )
    discord_output.send(webhook_url, msg)

    # Log each qualifying ticker to alerts_log
    for r in qualified:
        db.log_alert(r["ticker"], "ENTRY-CSP",
                     tier=r["tier"], score=r["score"])

    print(f"[ENTRY-CSP] Done — {len(qualified)} qualifying / {scanned} scanned")
    return len(qualified)


# ── Job 2: ENTRY-LEAP ────────────────────────────────────────────────────────

def run_entry_leap(schwab_headers: dict, webhook_url: str) -> int:
    print(f"\n{'='*60}\n[ENTRY-LEAP] Trade Alerts scan")

    if macro_calendar.macro_is_stale(max_age_days=7):
        macro_calendar.refresh_macro_calendar()

    vix = _get_vix()
    t1_th, t2_th = scoring.tier_thresholds(vix)
    print(f"  VIX={vix} | thresholds T1>={t1_th} T2>={t2_th}")

    candidates: list[dict] = []
    scanned = 0
    for ticker in universe.LEAP_SCAN:
        scanned += 1
        try:
            time.sleep(0.35)
            c = _evaluate_candidate(schwab_headers, ticker, vix, TARGET_EXPIRY)
            if c is None or not c["passed"] or c["tier"] is None:
                continue

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
            db.log_alert(c["ticker"], "ENTRY-LEAP",
                         tier=c["tier"], score=c["score"])
            sent += 1
            time.sleep(0.5)

    # Always send scan summary
    summary = discord_output.scan_summary_message(
        "ENTRY-LEAP", scanned, sent,
        extras=f"T1={t1_count} T2={t2_count}"
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
