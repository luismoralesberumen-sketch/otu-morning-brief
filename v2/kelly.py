"""
OTU Wheel v2.0 — Kelly Criterion ranking

Kelly fraction for a binary outcome:
    f* = (b*p - q) / b
where
    b = payoff ratio = win_amount / risk_amount
    p = P(win)
    q = 1 - p

For short puts in the OTU Wheel:
    win_amount  = premium (ROI%)
    risk_amount = assigned-loss estimate (max_loss_pct)
                  default = strike * (1 - stop_loss_floor)

We use Kelly to *rank* candidates — larger f* means more edge per
unit of risk. We DO NOT size positions to full Kelly (typical practice:
quarter-Kelly for fat-tail protection).

kelly_score(roi, wr, max_loss) returns f* * roi * 100 so the unit is
"expected premium retained per unit of capital-at-risk, in bps-ish".
"""

from __future__ import annotations


def kelly_fraction(roi_pct: float, win_rate_pct: float,
                    max_loss_pct: float) -> float:
    """
    Pure Kelly fraction.
      roi_pct       — premium as % of capital at risk (e.g. 3.5)
      win_rate_pct  — historical win rate (e.g. 72)
      max_loss_pct  — worst-case loss as % (e.g. 15)
    Returns f* (can be negative → no edge).
    """
    if max_loss_pct <= 0 or roi_pct <= 0:
        return 0.0
    b = roi_pct / max_loss_pct
    p = max(0.0, min(1.0, win_rate_pct / 100.0))
    q = 1.0 - p
    if b == 0:
        return 0.0
    return (b * p - q) / b


def kelly_score(roi_pct: float, win_rate_pct: float,
                max_loss_pct: float = 15.0) -> float:
    """
    Ranking metric: f* * roi. Higher is better.
    Zero or negative => no edge, filter from ranking.
    """
    f = kelly_fraction(roi_pct, win_rate_pct, max_loss_pct)
    if f <= 0:
        return 0.0
    return round(f * roi_pct * 100.0, 2)


def kelly_details(roi_pct: float, win_rate_pct: float,
                  max_loss_pct: float = 15.0) -> dict:
    """Debug payload — used in Discord footer when requested."""
    f = kelly_fraction(roi_pct, win_rate_pct, max_loss_pct)
    return {
        "roi_pct":      round(roi_pct, 2),
        "win_rate_pct": round(win_rate_pct, 1),
        "max_loss_pct": round(max_loss_pct, 1),
        "f_star":       round(f, 4),
        "score":        round(max(f, 0.0) * roi_pct * 100.0, 2),
    }
