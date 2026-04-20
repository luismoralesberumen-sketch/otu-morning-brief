"""
OTU Wheel v2.0 — Covered Call watchlist scanner

Reads v2/watchlist.json and scans each ticker for OTM call opportunities near
its target_delta. Same hard-filter + Kelly machinery as the PUT entry flow.

Output: one batch Discord message ranked by Kelly.
"""

from __future__ import annotations

import json
import os
import time
import datetime as _dt
from typing import Optional

import pytz

from . import (
    db, iv_rank, macro_calendar, scoring, filters, kelly,
    fundamentals, schwab_client, discord_output,
)


ET = pytz.timezone("America/New_York")
_HERE = os.path.dirname(os.path.abspath(__file__))
WATCHLIST_PATH = os.path.join(_HERE, "watchlist.json")


def load_watchlist() -> list[dict]:
    try:
        with open(WATCHLIST_PATH) as f:
            data = json.load(f)
        return data.get("covered_calls", [])
    except Exception as e:
        print(f"  [CC] watchlist load error: {e}")
        return []


def _evaluate_cc(schwab_headers: dict, entry: dict,
                 vix: Optional[float], target_expiry: str) -> Optional[dict]:
    """
    Runs the CC-side pipeline for one watchlist entry. Returns dict with
    strike, mid, ROI, Kelly, score, tier, flags. None on error.
    """
    ticker = entry["ticker"]
    target_delta = float(entry.get("target_delta", 0.25))

    candles = schwab_client.get_daily_candles(schwab_headers, ticker)
    if len(candles) < 50:
        return None
    closes = [c["close"] for c in candles]
    price  = closes[-1]

    ivr = iv_rank.compute_iv_rank(schwab_headers, ticker)

    # Call leg near target expiry + target delta
    opt = schwab_client.get_call_chain_near_delta(
        schwab_headers, ticker, target_expiry, target_delta=target_delta
    )
    if opt is None:
        return None

    fund = fundamentals.get_fundamentals(ticker)

    spread_pct = None
    if opt["mid"] > 0 and opt["ask"] > 0:
        spread_pct = (opt["ask"] - opt["bid"]) / opt["mid"] * 100.0

    # Hard filters (reuse PUT gates; earnings-sigma rule is side-agnostic here)
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

    inp = scoring.ConvictionInputs(
        price=price, closes=closes, candles=candles,
        iv_rank=ivr, pe_positive=fund["pe_positive"], beats_4q=fund["beats_4q"],
        open_interest=opt["open_interest"], spread_pct_of_mid=spread_pct,
    )
    base_score, details = scoring.calc_conviction(inp)
    score = scoring.apply_vix_modifier(base_score, vix)
    tier, tier_desc = scoring.classify_tier(score, vix)

    wr = details.get("backtest_wr", 50)
    kelly_s = kelly.kelly_score(opt["roi"], wr, max_loss_pct=15.0)

    dte = (_dt.date.fromisoformat(opt["expiry"]) - _dt.date.today()).days

    # Assignment distance: how far OTM the call is (% above current price)
    otm_pct = (opt["strike"] - price) / price * 100.0 if price else 0.0

    # Cost-basis guard: strike < cost means assignment locks in a loss
    cb = float(entry.get("cost_basis", 0) or 0)
    below_cost = bool(cb and opt["strike"] < cb)
    cb_buffer_pct = ((opt["strike"] - cb) / cb * 100.0) if cb else None

    return {
        "ticker":       ticker,
        "shares":       int(entry.get("shares", 0) or 0),
        "cost_basis":   float(entry.get("cost_basis", 0) or 0),
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
        "otm_pct":      round(otm_pct, 2),
        "below_cost":   below_cost,
        "cb_buffer_pct": round(cb_buffer_pct, 2) if cb_buffer_pct is not None else None,
        "score":        score,
        "tier":         tier,
        "tier_desc":    tier_desc,
        "flags":        flags,
        "passed":       passed,
        "backtest_wr":  wr,
        "note":         entry.get("note", ""),
    }


def run_entry_cc(schwab_headers: dict, webhook_url: str,
                 target_expiry: Optional[str] = None) -> int:
    """Scan watchlist and dispatch covered-call candidates batch."""
    from .engine import _get_vix, get_target_expiry
    te = target_expiry or get_target_expiry()

    print(f"\n{'='*60}\n[ENTRY-CC] Covered-Call watchlist scan")
    watch = load_watchlist()
    if not watch:
        print("  [CC] empty watchlist — nothing to scan")
        discord_output.send(
            webhook_url,
            discord_output.scan_summary_message("ENTRY-CC", 0, 0, extras="empty watchlist"),
        )
        return 0

    vix = _get_vix()
    print(f"  VIX={vix} | scanning {len(watch)} tickers")

    results: list[dict] = []
    for entry in watch:
        try:
            time.sleep(0.35)
            c = _evaluate_cc(schwab_headers, entry, vix, te)
            if c is None:
                print(f"  {entry['ticker']}: no data")
                continue
            results.append(c)
            print(f"  {entry['ticker']}: strike={c['strike']} roi={c['roi']}% ivr={c['iv_rank']} passed={c['passed']}")
        except Exception as e:
            print(f"  {entry['ticker']}: error {e}")

    # Sort: passed-and-kelly first, then by Kelly desc
    results.sort(key=lambda r: (not r["passed"], -(r.get("kelly") or 0)))

    msg = discord_output.cc_watchlist_message(
        results=results, vix=vix, target_expiry=te, total=len(watch),
    )
    discord_output.send(webhook_url, msg)

    # Log one alert per ticker that passed — dedupe 24h
    for r in results:
        if r["passed"] and r.get("kelly", 0) > 0:
            db.log_alert(r["ticker"], "ENTRY-CC",
                         tier=r.get("tier"), score=r.get("score"))

    print(f"[ENTRY-CC] Done — {len(results)} evaluated")
    return len(results)
