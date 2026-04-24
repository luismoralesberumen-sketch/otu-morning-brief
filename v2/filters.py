"""
OTU Wheel v2.0 — Hard Filters

Mandatory gates applied BEFORE scoring. Any failure -> skip alert entirely.

Returns (passed: bool, flags: list[str]).
Flags are human-readable reasons surfaced in Discord for skipped-but-interesting
candidates and to display warnings on alerts that just barely pass.

Filters:
  F1. IV Rank >= 30                     (premium-selling edge)
  F2. Put OI >= 100                     (minimum liquidity)
  F3. Spread (bid-ask) <= 5% of mid     (execution quality)
  F4. Earnings-vs-expiry conflict:
      if earnings lands inside option's lifetime
      AND strike > (price - 1 * 20d_stdev)
      → reject (too close to being ITM on earnings gap)
  F5. Macro event in next 24h          (FOMC/CPI/NFP/PPI/JOBS)
"""

from __future__ import annotations

import math
import datetime as _dt
from typing import Optional

from . import db, macro_calendar


# ── Individual filter helpers ────────────────────────────────────────────────

def f_iv_rank(iv_rank: Optional[float], min_rank: float = 30.0) -> tuple[bool, str]:
    if iv_rank is None:
        return False, "IV_RANK_UNAVAILABLE"
    if iv_rank < min_rank:
        return False, f"IV_RANK_LOW({iv_rank:.0f}<{min_rank:.0f})"
    return True, ""


def f_open_interest(oi: Optional[int], min_oi: int = 50) -> tuple[bool, str]:
    if oi is None:
        return False, "OI_UNAVAILABLE"
    if oi < min_oi:
        return False, f"OI_LOW({oi}<{min_oi})"
    return True, ""


def f_spread(bid: Optional[float], ask: Optional[float],
             max_pct: float = 10.0,
             iv_rank: Optional[float] = None) -> tuple[bool, str]:
    """
    Bid/ask spread gate. Default cap is 10% of mid.
    IVR override: names with IVR >= 65 get a relaxed cap of 15% — high-IVR
    names often carry wider spreads structurally and the premium edge
    compensates. Only opens the door, does not lower it.
    """
    if bid is None or ask is None or ask <= 0:
        return False, "SPREAD_UNAVAILABLE"
    mid = (bid + ask) / 2
    if mid <= 0:
        return False, "SPREAD_BAD_MID"
    spread_pct = (ask - bid) / mid * 100.0
    cap = max_pct
    if iv_rank is not None and iv_rank >= 65.0:
        cap = max(cap, 15.0)
    if spread_pct > cap:
        return False, f"SPREAD_WIDE({spread_pct:.1f}%>{cap:.0f}%)"
    return True, ""


def stdev_20d(closes: list[float]) -> Optional[float]:
    if len(closes) < 21:
        return None
    w = closes[-20:]
    mean = sum(w) / 20
    var = sum((x - mean) ** 2 for x in w) / 19
    return math.sqrt(var)


def f_earnings_vs_expiry(earnings_date: Optional[str], expiry: str,
                          strike: float, price: float,
                          closes: list[float],
                          sigma_buffer: float = 1.5) -> tuple[bool, str]:
    """
    Reject if earnings lands between today and option expiry AND strike is
    above (price - sigma_buffer * 20d stdev). Buffer widened from 1.0σ → 1.5σ
    to account for earnings gaps that routinely exceed 1σ moves.
    """
    if not earnings_date:
        return True, ""
    try:
        e_date = _dt.date.fromisoformat(earnings_date)
        x_date = _dt.date.fromisoformat(expiry)
    except Exception:
        return True, ""
    today = _dt.date.today()
    if not (today <= e_date <= x_date):
        return True, ""  # earnings not in window → no conflict

    sigma = stdev_20d(closes)
    if sigma is None or sigma <= 0:
        # Can't evaluate — be safe, reject
        return False, f"EARNINGS_IN_WINDOW({earnings_date})_NO_SIGMA"

    safe_strike = price - sigma_buffer * sigma
    if strike > safe_strike:
        return False, f"EARNINGS_RISK(strike>{safe_strike:.2f}=price-{sigma_buffer:.1f}σ)"
    return True, ""


def f_macro_window(hours: int = 24) -> tuple[bool, str]:
    ev = macro_calendar.has_event_within(hours=hours)
    if ev:
        names = ",".join(e["event_type"] for e in ev[:3])
        return False, f"MACRO_NEXT_{hours}H({names})"
    return True, ""


# ── Composite ────────────────────────────────────────────────────────────────

def passes_hard_filters(
    *,
    iv_rank:       Optional[float],
    open_interest: Optional[int],
    bid:           Optional[float],
    ask:           Optional[float],
    strike:        float,
    price:         float,
    expiry:        str,
    earnings_date: Optional[str],
    closes:        list[float],
) -> tuple[bool, list[str]]:
    """
    Returns (passed_all, flags).
    flags always contains every failing reason (not just the first) so the
    Discord log can explain why a near-miss was skipped.
    """
    flags: list[str] = []

    for passed, flag in (
        f_iv_rank(iv_rank),
        f_open_interest(open_interest),
        f_spread(bid, ask, iv_rank=iv_rank),
        f_earnings_vs_expiry(earnings_date, expiry, strike, price, closes),
        f_macro_window(hours=24),
    ):
        if not passed:
            flags.append(flag)

    return len(flags) == 0, flags
