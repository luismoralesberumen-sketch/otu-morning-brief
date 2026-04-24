"""
OTU Wheel v2.1 — Outcome Evaluator (Camino B)

Runs daily. For every ENTRY-* alert in alerts_log with a snapshot (strike,
expiry, mid, delta, price_at_alert), evaluates the outcome at the next
unfilled checkpoint (T+7 / T+14 / T+21 / T+30 / at_expiry) using the current
price of the underlying.

Checkpoints missed (e.g. we went 9 days without running) are back-filled on
the next run using the historical daily candle at that date.

Outcome classes (computed per side):

  PUT (CSP):
    OTM_SAFE       — price_at_eval >= strike * 1.03  (3% cushion)
    ITM_TOUCH      — strike <= price_at_eval < strike * 1.03
    BREACHED       — price_at_eval < strike
    EXPIRED_OTM    — at-or-past expiry AND price >= strike
    EXPIRED_ITM    — at-or-past expiry AND price <  strike

  CALL (CC / LEAP):
    OTM_SAFE       — price_at_eval <= strike * 0.97
    ITM_TOUCH      — strike * 0.97 < price_at_eval <= strike
    BREACHED       — price_at_eval > strike
    EXPIRED_OTM    — at-or-past expiry AND price <= strike
    EXPIRED_ITM    — at-or-past expiry AND price >  strike

pnl_est_pct is a rough estimate on premium basis using a linear decay
approximation — good enough for calibration, not for accounting.
"""

from __future__ import annotations

import datetime as _dt
from typing import Optional

from . import db, schwab_client


CHECKPOINTS = (7, 14, 21, 30)
AT_EXPIRY_CHECKPOINT = 99  # sentinel days_since value


def _historical_close(schwab_headers: dict, ticker: str,
                       target_date: _dt.date) -> Optional[float]:
    """Return the close on target_date or the nearest prior trading day."""
    try:
        candles = schwab_client.get_daily_candles(schwab_headers, ticker, years=1)
    except Exception:
        return None
    if not candles:
        return None
    target_iso = target_date.isoformat()
    # candles expected sorted ascending with 'date' (ISO) and 'close'
    last_close = None
    for c in candles:
        d = c.get("date") or c.get("datetime") or ""
        if d[:10] <= target_iso:
            last_close = c.get("close")
        else:
            break
    return last_close


def _classify(side: str, strike: float, price: float,
              past_expiry: bool) -> str:
    if side == "PUT":
        if past_expiry:
            return "EXPIRED_OTM" if price >= strike else "EXPIRED_ITM"
        if price >= strike * 1.03: return "OTM_SAFE"
        if price >= strike:        return "ITM_TOUCH"
        return "BREACHED"
    # CALL
    if past_expiry:
        return "EXPIRED_OTM" if price <= strike else "EXPIRED_ITM"
    if price <= strike * 0.97: return "OTM_SAFE"
    if price <= strike:        return "ITM_TOUCH"
    return "BREACHED"


def _pnl_estimate(side: str, strike: float, price: float, mid: Optional[float],
                   price_at_alert: Optional[float], days_since: int,
                   dte_total: int, past_expiry: bool) -> Optional[float]:
    """
    Rough P/L on premium basis.
      • If past_expiry: OTM → +100%, ITM → intrinsic loss vs premium
      • If not expired: linear theta decay proxy × directional adjust
    Returns % of premium (e.g. +50.0 = booked 50% of max profit).
    """
    if mid is None or mid <= 0:
        return None
    if past_expiry:
        if side == "PUT":
            if price >= strike:
                return 100.0
            intrinsic = strike - price
            return round((mid - intrinsic) / mid * 100.0, 1)
        else:
            if price <= strike:
                return 100.0
            intrinsic = price - strike
            return round((mid - intrinsic) / mid * 100.0, 1)
    # Rough theta decay (fraction of time elapsed)
    if dte_total <= 0:
        return None
    time_decay = min(1.0, max(0.0, days_since / dte_total))
    # Directional penalty: if breached, more aggressive loss
    if side == "PUT":
        dist_pct = (price - strike) / strike * 100.0 if strike else 0
    else:
        dist_pct = (strike - price) / strike * 100.0 if strike else 0
    # base: we've earned time_decay * 100% of theta, minus adverse move
    base_pnl = time_decay * 100.0
    if dist_pct < 0:  # breached
        base_pnl -= abs(dist_pct) * 3  # rough gamma amplification
    return round(base_pnl, 1)


def evaluate_pending(schwab_headers: dict, verbose: bool = True) -> int:
    """
    Main entry point — call once per day from scheduler.
    Returns number of outcome rows written.
    """
    today = _dt.date.today()
    alerts = db.alerts_pending_evaluation(days_since_min=7)
    written = 0

    for a in alerts:
        alert_id = a["id"]
        ticker   = a["ticker"]
        side     = a["side"]
        strike   = a["strike"]
        expiry   = a["expiry"]
        mid      = a["mid_at_alert"]
        price0   = a["price_at_alert"]

        if not (ticker and side and strike and expiry):
            continue
        try:
            alert_dt = _dt.date.fromisoformat(a["timestamp"][:10])
            exp_dt   = _dt.date.fromisoformat(expiry)
        except Exception:
            continue

        existing = {o["days_since"] for o in db.outcomes_for_alert(alert_id)}
        dte_total = max(1, (exp_dt - alert_dt).days)

        # Build checkpoint list: T+7/14/21/30 + at-expiry
        targets = [(d, alert_dt + _dt.timedelta(days=d)) for d in CHECKPOINTS]
        if today >= exp_dt:
            targets.append((AT_EXPIRY_CHECKPOINT, exp_dt))

        for days_since, eval_date in targets:
            if days_since in existing:
                continue
            if eval_date > today:
                continue  # future checkpoint
            # Fetch price — current if today, historical otherwise
            if eval_date == today:
                q = schwab_client.get_daily_candles(schwab_headers, ticker, years=1)
                price_eval = q[-1]["close"] if q else None
            else:
                price_eval = _historical_close(schwab_headers, ticker, eval_date)
            if price_eval is None:
                continue

            past_expiry = (days_since == AT_EXPIRY_CHECKPOINT) or (eval_date >= exp_dt)
            pct_to_strike = (price_eval - strike) / strike * 100.0 if strike else None
            klass = _classify(side, strike, price_eval, past_expiry)
            pnl = _pnl_estimate(side, strike, price_eval, mid, price0,
                                days_since if days_since != AT_EXPIRY_CHECKPOINT else dte_total,
                                dte_total, past_expiry)

            db.upsert_outcome(
                alert_id=alert_id, days_since=days_since,
                eval_date=eval_date.isoformat(),
                price_at_eval=round(price_eval, 2),
                pct_to_strike=round(pct_to_strike, 2) if pct_to_strike is not None else None,
                outcome_class=klass, pnl_est_pct=pnl,
            )
            written += 1
            if verbose:
                print(f"  [OUTCOME] {ticker} {side} K={strike} exp={expiry} "
                      f"T+{days_since}d: {klass} price={price_eval:.2f} "
                      f"({pct_to_strike:+.2f}%) pnl~{pnl}")

    print(f"[OUTCOMES] wrote {written} rows across {len(alerts)} alerts")
    return written
