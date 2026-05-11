"""
OTU Wheel v2.1 — Weekly Vertical Spreads Scanner

Scans same 40-ticker universe as CSP/LEAP. Targets next Friday expiry (DTE 4-7).
Runs Mon-Wed only — Thursday/Friday weekly premium has collapsed.

Bias:
  BULL (3+ pts): price > EMA200 (2pt), price > EMA50 (1pt),
                 RSI 35-55 (1pt), MACD turning up (1pt)
  BEAR (3+ pts): price < EMA200 (2pt), price < EMA50 (1pt),
                 RSI 60-75 (1pt), MACD declining (1pt)

Spread type:
  BULL + IVR >= 20 → BPS  (Bull Put Spread, credit)
  BEAR + IVR >= 20 → BCS  (Bear Call Spread, credit)
  credit IVR < 20  → debit spread, requires T1 score (>= 70)

Entry gates:
  Credit >= 25% of spread width
  Short leg OI >= 50
  No earnings in window
"""

from __future__ import annotations

import time
import datetime as _dt
from typing import Optional

from . import scoring, schwab_client, iv_rank, fundamentals, db, discord_output, universe


_MIN_CREDIT_PCT  = 25.0
_MIN_OI          = 50
_IVR_CREDIT_GATE = 20.0
_T1_SCORE        = 70
_T2_SCORE        = 55


def get_weekly_expiry() -> str:
    """Next Friday from today (DTE 4-7). If today is Friday, targets next week."""
    today = _dt.date.today()
    days_ahead = (4 - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return (today + _dt.timedelta(days=days_ahead)).isoformat()


def determine_spread_bias(closes: list[float]) -> Optional[str]:
    """Returns "BULL", "BEAR", or None. Requires 3+ matching points per side."""
    if len(closes) < 210:
        return None

    price       = closes[-1]
    ema50_list  = scoring.calc_ema(closes, 50)
    ema200_list = scoring.calc_ema(closes, 200)
    if not ema50_list or not ema200_list:
        return None
    ema50  = ema50_list[-1]
    ema200 = ema200_list[-1]
    rsi    = scoring.calc_rsi(closes)
    if rsi is None:
        return None
    macd = scoring.calc_macd(closes)

    bull = 0
    bear = 0

    if price > ema200: bull += 2
    else:              bear += 2
    if price > ema50:  bull += 1
    else:              bear += 1

    if 35 <= rsi <= 55:   bull += 1
    elif 60 <= rsi <= 75: bear += 1
    elif rsi > 75:        bear += 2

    if macd:
        if macd.get("rising") and macd.get("bullish"): bull += 1
        elif not macd.get("ok"):                       bear += 1

    if bull >= 3 and bull > bear: return "BULL"
    if bear >= 3 and bear > bull: return "BEAR"
    return None


def score_spread_setup(closes: list[float], price: float,
                       iv_rank_val: Optional[float],
                       bias: str) -> tuple[int, dict]:
    """
    0-100 spread-specific score.
    30 pts — Trend alignment
    25 pts — RSI zone quality
    25 pts — MACD momentum
    20 pts — IVR
    """
    details: dict = {}
    score = 0

    ema50_list  = scoring.calc_ema(closes, 50)
    ema200_list = scoring.calc_ema(closes, 200)
    ema50  = ema50_list[-1]  if ema50_list  else None
    ema200 = ema200_list[-1] if ema200_list else None
    rsi  = scoring.calc_rsi(closes)
    macd = scoring.calc_macd(closes)

    details["ema50"]     = round(ema50,  2) if ema50  else None
    details["ema200"]    = round(ema200, 2) if ema200 else None
    details["rsi"]       = round(rsi, 2) if rsi else None
    details["macd_ok"]   = macd.get("ok")   if macd else None
    details["macd_hist"] = macd.get("hist") if macd else None
    details["iv_rank"]   = round(iv_rank_val, 1) if iv_rank_val else None

    # Trend (30 pts)
    if bias == "BULL":
        if ema200 and price > ema200: score += 20
        if ema50  and price > ema50:  score += 10
    else:
        if ema200 and price < ema200: score += 20
        if ema50  and price < ema50:  score += 10
        # Rejection near EMA200 from above (within 3%) — bonus for bear setups
        if ema200 and 0 < (price - ema200) / ema200 < 0.03: score += 10

    # RSI zone (25 pts)
    if rsi:
        if bias == "BULL":
            if 38 <= rsi <= 52:  score += 25
            elif 32 <= rsi < 38: score += 18
            elif 52 < rsi <= 58: score += 12
        else:
            if 62 <= rsi <= 72:  score += 25
            elif 72 < rsi <= 80: score += 18
            elif 58 <= rsi < 62: score += 12

    # MACD (25 pts)
    if macd:
        if bias == "BULL":
            if macd.get("rising") and macd.get("bullish"):  score += 25
            elif macd.get("rising") or macd.get("bullish"): score += 15
        else:
            hist = macd.get("hist", 0) or 0
            if not macd.get("ok"):                         score += 25
            elif not macd.get("bullish") and hist < 0:     score += 15

    # IVR (20 pts)
    if iv_rank_val:
        if iv_rank_val >= 50:   score += 20
        elif iv_rank_val >= 35: score += 15
        elif iv_rank_val >= 20: score += 8

    return min(score, 100), details


def evaluate_spread_candidate(headers: dict, ticker: str,
                               target_expiry: str) -> Optional[dict]:
    """Full pipeline for one ticker. Returns result dict (passed=True/False) or None."""
    candles = schwab_client.get_daily_candles(headers, ticker)
    if len(candles) < 210:
        return None
    closes = [c["close"] for c in candles]
    price  = closes[-1]

    bias = determine_spread_bias(closes)
    if bias is None:
        return None

    ivr = iv_rank.compute_iv_rank(headers, ticker)
    score, details = score_spread_setup(closes, price, ivr, bias)

    base = {"ticker": ticker, "score": score, "bias": bias,
            "details": details, "price": round(price, 2), "iv_rank": ivr}

    if score < _T2_SCORE:
        return {**base, "passed": False, "reject_reason": f"SCORE_LOW({score}<{_T2_SCORE})"}

    use_credit   = (ivr or 0) >= _IVR_CREDIT_GATE
    chain_side   = "PUT"  if bias == "BULL" else "CALL"
    spread_label = ("BPS" if bias == "BULL" else "BCS") if use_credit else \
                   ("BCS_DEBIT" if bias == "BULL" else "BPS_DEBIT")

    if not use_credit and score < _T1_SCORE:
        return {**base, "passed": False,
                "reject_reason": f"DEBIT_NEEDS_T1(ivr={ivr:.0f}<{_IVR_CREDIT_GATE},score={score}<{_T1_SCORE})"}

    fund = fundamentals.get_fundamentals(ticker)
    earnings_date = fund.get("earnings_date")
    if earnings_date:
        try:
            e_date = _dt.date.fromisoformat(earnings_date)
            x_date = _dt.date.fromisoformat(target_expiry)
            if _dt.date.today() <= e_date <= x_date:
                return {**base, "passed": False,
                        "reject_reason": f"EARNINGS_IN_WINDOW({earnings_date})"}
        except Exception:
            pass

    legs = schwab_client.get_spread_legs(headers, ticker, target_expiry, side=chain_side)
    if legs is None:
        return {**base, "passed": False, "reject_reason": "NO_CHAIN"}

    if use_credit:
        if legs["credit"] <= 0:
            return {**base, "passed": False,
                    "reject_reason": f"DEBIT_SPREAD(credit={legs['credit']})"}
        if legs["credit_pct"] < _MIN_CREDIT_PCT:
            return {**base, "passed": False,
                    "reject_reason": f"CREDIT_LOW({legs['credit_pct']:.1f}%<{_MIN_CREDIT_PCT:.0f}%)"}

    if legs["short_oi"] < _MIN_OI:
        return {**base, "passed": False,
                "reject_reason": f"OI_LOW({legs['short_oi']}<{_MIN_OI})"}

    return {
        **base,
        "passed":       True,
        "flags":        [],
        "spread_type":  spread_label,
        "short_strike": legs["short_strike"],
        "long_strike":  legs["long_strike"],
        "credit":       legs["credit"],
        "width":        legs["width"],
        "credit_pct":   legs["credit_pct"],
        "short_delta":  legs["short_delta"],
        "expiry":       legs["expiry"],
        "dte":          legs["dte"],
        "short_oi":     legs["short_oi"],
        "tier":         1 if score >= _T1_SCORE else 2,
        "pe_positive":  fund["pe_positive"],
        "beats_4q":     fund["beats_4q"],
    }


def run_entry_spreads(schwab_headers: dict, webhook_url: str) -> int:
    """Weekly vertical spreads scan. Mon-Wed only, targets next Friday."""
    today = _dt.date.today()
    if today.weekday() >= 3:  # Thu=3, Fri=4
        print(f"[ENTRY-SPREADS] Skip — weekday {today.weekday()} (Mon-Wed only)")
        return 0

    target_expiry = get_weekly_expiry()
    dte_target    = (_dt.date.fromisoformat(target_expiry) - today).days
    print(f"\n{'='*60}\n[ENTRY-SPREADS] Weekly scan | target={target_expiry} DTE={dte_target}")

    qualified: list[dict] = []
    near_miss:  list[dict] = []
    scanned = 0

    for ticker in universe.ALL_40:
        scanned += 1
        try:
            time.sleep(0.35)
            c = evaluate_spread_candidate(schwab_headers, ticker, target_expiry)
            if c is None:
                print(f"  {ticker}: no data / no bias")
                continue
            if not c["passed"]:
                near_miss.append(c)
                print(f"  {ticker}: skip ({c['reject_reason']})")
                continue
            print(f"  {ticker}: {c['spread_type']} score={c['score']} "
                  f"credit=${c['credit']} ({c['credit_pct']}%) ivr={c['iv_rank']}")
            qualified.append(c)
        except Exception as e:
            print(f"  {ticker}: error {e}")

    qualified.sort(key=lambda r: r["score"], reverse=True)
    near_miss.sort(key=lambda r: r.get("score", 0), reverse=True)

    msg = discord_output.spread_scan_message(
        qualified=qualified, near_miss=near_miss[:8],
        scanned=scanned, target_expiry=target_expiry, dte=dte_target,
    )
    discord_output.send(webhook_url, msg)

    for r in qualified:
        db.log_alert(
            r["ticker"], "ENTRY-SPREAD",
            tier=r["tier"], score=r["score"],
            side=r["bias"],
            strike=r.get("short_strike"),
            expiry=r.get("expiry"),
            mid_at_alert=r.get("credit"),
            delta_at_alert=r.get("short_delta"),
            iv_rank_at_alert=r.get("iv_rank"),
            roi_at_alert=r.get("credit_pct"),
            price_at_alert=r.get("price"),
            long_strike=r.get("long_strike"),
            spread_width=r.get("width"),
            spread_type=r.get("spread_type"),
        )

    print(f"[ENTRY-SPREADS] Done — {len(qualified)} qualifying / {scanned} scanned")
    return len(qualified)
