"""
OTU Wheel v2.0 — IV Rank Calculator

IV Rank = (IV_current - IV_min_52w) / (IV_max_52w - IV_min_52w) * 100

NOTE ON METHODOLOGY
    Neither Schwab nor yfinance expose daily historical implied-vol snapshots
    for free. We approximate IV_min/IV_max using 20-day annualized realized
    volatility rolled over the past 252 trading days. Empirically this tracks
    ATM 30D IV within a few rank points, which is sufficient for the Wheel
    strategy filter (>= 30).

    IV_current is pulled live from Schwab's options chain — the ATM put
    closest to 30 DTE — because that's our actual trade candidate.

Cache policy:
    iv_cache table: one row per ticker, refreshed once per day (20h TTL).
    compute_iv_rank() checks staleness and rebuilds the row if needed.
"""

from __future__ import annotations

import math
import datetime as _dt
from typing import Optional

import requests

from . import db


_SCHWAB_PRICE_URL = "https://api.schwabapi.com/marketdata/v1/pricehistory"
_SCHWAB_CHAIN_URL = "https://api.schwabapi.com/marketdata/v1/chains"


# ── Historical volatility proxy (min/max over 252d) ──────────────────────────

def _fetch_daily_closes(schwab_headers: dict, ticker: str, days: int = 400) -> list[float]:
    """Pull daily closes from Schwab pricehistory. 400 calendar days ≈ 280 trading days."""
    r = requests.get(
        _SCHWAB_PRICE_URL,
        params={
            "symbol": ticker,
            "periodType": "year",
            "period": 2,
            "frequencyType": "daily",
            "frequency": 1,
            "needExtendedHoursData": "false",
        },
        headers=schwab_headers,
        timeout=15,
    )
    candles = r.json().get("candles", [])
    closes = [c["close"] for c in candles if c.get("close")]
    return closes[-days:] if len(closes) > days else closes


def _rolling_hv_series(closes: list[float], window: int = 20) -> list[float]:
    """
    Rolling window annualized realized-vol series.
    HV_t = stddev(log-returns[t-window..t]) * sqrt(252)
    Returns as a percentage (e.g. 0.45 = 45%).
    """
    if len(closes) < window + 2:
        return []
    # Log returns
    logrets = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0 and closes[i] > 0:
            logrets.append(math.log(closes[i] / closes[i - 1]))
        else:
            logrets.append(0.0)

    hv: list[float] = []
    for i in range(window, len(logrets) + 1):
        w = logrets[i - window:i]
        mean = sum(w) / window
        var = sum((x - mean) ** 2 for x in w) / (window - 1)
        hv.append(math.sqrt(var) * math.sqrt(252))
    return hv


def _fetch_current_iv(schwab_headers: dict, ticker: str) -> Optional[float]:
    """
    Fetch implied vol from the ATM put closest to 30 DTE in Schwab options chain.
    Returns IV as a decimal (e.g. 0.45 = 45%), or None if unavailable.
    """
    try:
        r = requests.get(
            _SCHWAB_CHAIN_URL,
            params={
                "symbol": ticker,
                "contractType": "PUT",
                "strikeCount": 10,
                "includeUnderlyingQuote": "true",
                "strategy": "SINGLE",
                "range": "NTM",              # Near the money
            },
            headers=schwab_headers,
            timeout=15,
        )
        chain = r.json()
        if chain.get("status") == "FAILED" or "putExpDateMap" not in chain:
            return None
        underlying = chain.get("underlyingPrice", 0)
        if not underlying:
            return None
        today = _dt.date.today()

        # Pick expiry closest to 30 DTE
        put_map = chain["putExpDateMap"]
        best_exp, best_diff = None, 999
        for exp_key in put_map:
            exp_date = _dt.date.fromisoformat(exp_key.split(":")[0])
            dte = (exp_date - today).days
            if dte < 7:
                continue
            diff = abs(dte - 30)
            if diff < best_diff:
                best_diff = diff
                best_exp = exp_key
        if best_exp is None:
            return None

        # Strike closest to underlying
        best_strike, best_sdiff = None, 1e9
        for strike_str in put_map[best_exp]:
            s = float(strike_str)
            sdiff = abs(s - underlying)
            if sdiff < best_sdiff:
                best_sdiff = sdiff
                best_strike = strike_str
        if best_strike is None:
            return None

        contract = put_map[best_exp][best_strike][0]
        iv_pct = contract.get("volatility")
        if iv_pct is None or iv_pct <= 0:
            return None
        # Schwab returns IV as percentage (e.g. 45.2). Normalize to decimal.
        return float(iv_pct) / 100.0
    except Exception as e:
        print(f"  [IV] current IV error {ticker}: {e}")
        return None


# ── Public API ───────────────────────────────────────────────────────────────

def refresh_iv_cache(schwab_headers: dict, ticker: str) -> Optional[dict]:
    """
    Rebuild iv_cache row for ticker. Returns dict with min/max/current or None.
    """
    closes = _fetch_daily_closes(schwab_headers, ticker, days=300)
    if len(closes) < 40:
        print(f"  [IV] {ticker}: insufficient history ({len(closes)} bars)")
        return None

    hv = _rolling_hv_series(closes, window=20)
    if not hv:
        return None
    # Restrict to last 252 trading days
    hv = hv[-252:] if len(hv) > 252 else hv
    iv_min = min(hv)
    iv_max = max(hv)

    iv_current = _fetch_current_iv(schwab_headers, ticker)

    db.set_iv_cache(ticker, iv_min=iv_min, iv_max=iv_max, iv_current=iv_current)
    return {"iv_min": iv_min, "iv_max": iv_max, "iv_current": iv_current}


def compute_iv_rank(schwab_headers: dict, ticker: str,
                    force_refresh: bool = False) -> Optional[float]:
    """
    Returns IV Rank (0-100) for ticker. Refreshes cache if stale (>20h old).
    Returns None if computation fails.
    """
    if force_refresh or db.iv_cache_is_stale(ticker):
        row = refresh_iv_cache(schwab_headers, ticker)
        if row is None:
            return None
        iv_min, iv_max, iv_current = row["iv_min"], row["iv_max"], row["iv_current"]
    else:
        cached = db.get_iv_cache(ticker)
        iv_min, iv_max = cached["iv_min_52w"], cached["iv_max_52w"]
        iv_current = cached["iv_current"]
        # iv_current can be stale-ish — refresh just the current leg
        fresh = _fetch_current_iv(schwab_headers, ticker)
        if fresh is not None:
            iv_current = fresh
            db.set_iv_cache(ticker, iv_min=iv_min, iv_max=iv_max, iv_current=iv_current)

    if iv_current is None or iv_max is None or iv_min is None:
        return None
    if iv_max <= iv_min:
        return 50.0  # degenerate range
    rank = (iv_current - iv_min) / (iv_max - iv_min) * 100.0
    return max(0.0, min(100.0, rank))


# ── Pure helper for testing ──────────────────────────────────────────────────

def iv_rank_from_values(iv_current: float, iv_min: float, iv_max: float) -> float:
    """Exposed for unit tests."""
    if iv_max <= iv_min:
        return 50.0
    r = (iv_current - iv_min) / (iv_max - iv_min) * 100.0
    return max(0.0, min(100.0, r))
