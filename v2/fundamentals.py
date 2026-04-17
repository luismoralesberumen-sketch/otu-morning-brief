"""
OTU Wheel v2.0 — Fundamentals via yfinance

Two-hour in-process cache (yfinance is slow and rate-limited).

For each ticker we expose:
    pe_positive   — trailing P/E > 0
    beats_4q      — last 4 quarters of earnings surprise all > 0
    earnings_date — next earnings date (ISO) or None
"""

from __future__ import annotations

import datetime as _dt
from typing import Optional

try:
    import yfinance as yf
except Exception:       # pragma: no cover
    yf = None  # type: ignore


_CACHE: dict[str, tuple[float, dict]] = {}   # ticker -> (ts, data)
_TTL_SEC = 2 * 3600


def _now() -> float:
    return _dt.datetime.utcnow().timestamp()


def _cache_get(ticker: str) -> Optional[dict]:
    entry = _CACHE.get(ticker)
    if not entry:
        return None
    ts, data = entry
    if _now() - ts > _TTL_SEC:
        return None
    return data


def _cache_put(ticker: str, data: dict) -> None:
    _CACHE[ticker] = (_now(), data)


def get_fundamentals(ticker: str) -> dict:
    """
    Returns:
      {
        "pe_positive":   bool,
        "beats_4q":      bool,
        "earnings_date": ISO str or None,
      }
    Never raises — on any error returns conservative defaults (False/None).
    """
    cached = _cache_get(ticker)
    if cached is not None:
        return cached

    data = {"pe_positive": False, "beats_4q": False, "earnings_date": None}
    if yf is None:
        _cache_put(ticker, data)
        return data

    try:
        t = yf.Ticker(ticker)

        # P/E
        try:
            info = t.info or {}
            pe = info.get("trailingPE") or info.get("forwardPE")
            data["pe_positive"] = bool(pe and pe > 0)
        except Exception:
            pass

        # Earnings beats: last 4 quarters where Surprise% > 0
        try:
            eh = t.earnings_history
            if eh is not None and not eh.empty:
                tail = eh.tail(4)
                surprises = tail.get("surprisePercent")
                if surprises is not None:
                    vals = [v for v in surprises.tolist() if v is not None]
                    if len(vals) >= 3:
                        data["beats_4q"] = all(v > 0 for v in vals[-4:])
        except Exception:
            pass

        # Next earnings date
        try:
            cal = t.calendar
            if cal is not None:
                ed = None
                if isinstance(cal, dict):
                    ed = cal.get("Earnings Date")
                else:
                    try:
                        ed = cal.loc["Earnings Date"].iloc[0]
                    except Exception:
                        ed = None
                if ed is not None:
                    # ed may be a list / Timestamp / datetime
                    if isinstance(ed, (list, tuple)) and ed:
                        ed = ed[0]
                    if hasattr(ed, "strftime"):
                        data["earnings_date"] = ed.strftime("%Y-%m-%d")
                    elif isinstance(ed, str):
                        data["earnings_date"] = ed[:10]
        except Exception:
            pass

    except Exception as e:
        print(f"  [FUND] {ticker}: {e}")

    _cache_put(ticker, data)
    return data
