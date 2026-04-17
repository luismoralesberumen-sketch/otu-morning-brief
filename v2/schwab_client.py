"""
OTU Wheel v2.0 — Schwab API client helpers

Thin wrapper around the endpoints we use. No caching here (caller decides).
Token refresh and env management stays in main.py / refresh_schwab_token().
"""

from __future__ import annotations

import time
import datetime as _dt
from typing import Optional

import requests


_BASE      = "https://api.schwabapi.com"
_QUOTES    = f"{_BASE}/marketdata/v1/quotes"
_HISTORY   = f"{_BASE}/marketdata/v1/pricehistory"
_CHAINS    = f"{_BASE}/marketdata/v1/chains"
_ACCOUNTS  = f"{_BASE}/trader/v1/accounts"


# ── Quotes ───────────────────────────────────────────────────────────────────

def get_mark_price(headers: dict, ticker: str) -> Optional[float]:
    """mark = bid/ask mid — never use closePrice (yesterday's close)."""
    try:
        r = requests.get(_QUOTES, params={"symbols": ticker, "fields": "quote"},
                         headers=headers, timeout=10)
        q = r.json().get(ticker, {}).get("quote", {})
        p = q.get("mark") or q.get("lastPrice")
        return float(p) if p else None
    except Exception as e:
        print(f"  [SCHWAB] mark error {ticker}: {e}")
        return None


# ── Daily candles ────────────────────────────────────────────────────────────

def get_daily_candles(headers: dict, ticker: str, years: int = 2,
                      inject_live_close: bool = True) -> list[dict]:
    try:
        r = requests.get(
            _HISTORY,
            params={
                "symbol": ticker,
                "periodType": "year",
                "period": years,
                "frequencyType": "daily",
                "frequency": 1,
                "needExtendedHoursData": "false",
            },
            headers=headers,
            timeout=15,
        )
        candles = r.json().get("candles", [])
        if inject_live_close and candles:
            live = get_mark_price(headers, ticker)
            if live:
                last = dict(candles[-1])
                last["close"] = live
                last["high"] = max(last.get("high", live), live)
                last["low"]  = min(last.get("low",  live), live)
                candles[-1] = last
        return candles
    except Exception as e:
        print(f"  [SCHWAB] history error {ticker}: {e}")
        return []


# ── Options chain ────────────────────────────────────────────────────────────

def get_put_chain_near_delta(headers: dict, ticker: str,
                             target_expiry: str,
                             target_delta: float = 0.30) -> Optional[dict]:
    """
    Returns a dict with the single best put contract near (target_delta, target_expiry):
        { strike, expiry, bid, ask, mid, iv, delta, roi, open_interest, underlying }
    Returns None if unavailable.
    """
    try:
        r = requests.get(
            _CHAINS,
            params={
                "symbol": ticker,
                "contractType": "PUT",
                "strikeCount": 30,
                "includeUnderlyingQuote": "true",
                "strategy": "SINGLE",
                "range": "OTM",
            },
            headers=headers,
            timeout=15,
        )
        chain = r.json()
        if chain.get("status") == "FAILED" or "putExpDateMap" not in chain:
            return None
        underlying = chain.get("underlyingPrice", 0)
        if not underlying:
            return None

        today = _dt.date.today()
        try:
            target_date = _dt.date.fromisoformat(target_expiry)
        except Exception:
            target_date = today + _dt.timedelta(days=30)
        target_dte = max(7, (target_date - today).days)

        # Expiry closest to target
        best_exp, best_diff = None, 999
        for exp_key in chain["putExpDateMap"]:
            exp_date = _dt.date.fromisoformat(exp_key.split(":")[0])
            dte = (exp_date - today).days
            if dte < 7:
                continue
            diff = abs(dte - target_dte)
            if diff < best_diff:
                best_diff = diff
                best_exp = exp_key
        if best_exp is None:
            return None

        # Strike closest to target_delta
        best_contract, best_dd = None, 999
        for strike_str, contracts in chain["putExpDateMap"][best_exp].items():
            c = contracts[0]
            d = abs(c.get("delta", 0))
            if not d:
                continue
            diff = abs(d - target_delta)
            if diff < best_dd:
                best_dd = diff
                best_contract = (float(strike_str), c)
        if best_contract is None:
            return None

        strike, c = best_contract
        bid = float(c.get("bid", 0) or 0)
        ask = float(c.get("ask", 0) or 0)
        mid = round((bid + ask) / 2, 2)
        return {
            "underlying":    round(underlying, 2),
            "expiry":        best_exp.split(":")[0],
            "strike":        strike,
            "bid":           bid,
            "ask":           ask,
            "mid":           mid,
            "iv":            float(c.get("volatility", 0) or 0),
            "delta":         round(abs(c.get("delta", 0) or 0), 2),
            "roi":           round(mid / strike * 100, 2) if strike else 0.0,
            "open_interest": int(c.get("openInterest", 0) or 0),
        }
    except Exception as e:
        print(f"  [SCHWAB] chain error {ticker}: {e}")
        return None


# ── Account positions (for MANAGE module) ────────────────────────────────────

def get_accounts(headers: dict) -> list[dict]:
    try:
        r = requests.get(f"{_ACCOUNTS}/accountNumbers", headers=headers, timeout=15)
        return r.json() or []
    except Exception as e:
        print(f"  [SCHWAB] accounts error: {e}")
        return []


def get_positions(headers: dict, account_hash: str) -> list[dict]:
    """
    Returns raw positions list from Schwab trader API.
    Includes options — each position has instrument.assetType == 'OPTION'.
    """
    try:
        r = requests.get(
            f"{_ACCOUNTS}/{account_hash}",
            params={"fields": "positions"},
            headers=headers,
            timeout=15,
        )
        j = r.json() or {}
        sa = j.get("securitiesAccount", {})
        return sa.get("positions", [])
    except Exception as e:
        print(f"  [SCHWAB] positions error: {e}")
        return []


def get_option_quote(headers: dict, option_symbol: str) -> Optional[dict]:
    """Quote for a single option contract by OSI symbol. Returns mark (per share)."""
    try:
        r = requests.get(
            _QUOTES,
            params={"symbols": option_symbol, "fields": "quote"},
            headers=headers, timeout=10
        )
        q = r.json().get(option_symbol, {}).get("quote", {})
        return {
            "mark":  float(q.get("mark", 0) or 0),
            "bid":   float(q.get("bidPrice", 0) or 0),
            "ask":   float(q.get("askPrice", 0) or 0),
            "delta": float(q.get("delta", 0) or 0),
        }
    except Exception as e:
        print(f"  [SCHWAB] option quote error {option_symbol}: {e}")
        return None
