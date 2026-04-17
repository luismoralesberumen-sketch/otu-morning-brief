"""
OTU v2.0 — Integration tests with mocked Schwab responses.

Validates end-to-end pipeline:
  daily candles → IV rank → conviction → filters → Kelly → tier → dedupe

Run:
    python -m v2.tests.test_integration
"""

from __future__ import annotations

import os
import sys
import math
import tempfile
import datetime as _dt
from unittest.mock import patch, MagicMock

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


# ── Fake Schwab data builders ────────────────────────────────────────────────

def build_candles(n: int = 260, start: float = 100.0,
                   drift: float = 0.0005, vol: float = 0.012) -> list[dict]:
    """Geometric-brownian-ish daily candles."""
    out = []
    p = start
    import random
    random.seed(42)
    for i in range(n):
        ret = drift + vol * (random.random() - 0.5) * 2
        p = max(1.0, p * (1 + ret))
        out.append({
            "datetime": i,
            "open": p * 0.99, "high": p * 1.01, "low": p * 0.98,
            "close": p, "volume": 1_000_000 + i * 100,
        })
    return out


def build_chain_response(strike: float = 95.0, delta: float = -0.30,
                         bid: float = 2.80, ask: float = 2.90,
                         iv_pct: float = 32.0, oi: int = 500,
                         underlying: float = 100.0,
                         expiry: str = "2026-05-15") -> dict:
    return {
        "status": "SUCCESS",
        "underlyingPrice": underlying,
        "putExpDateMap": {
            f"{expiry}:28": {
                str(strike): [{
                    "bid": bid, "ask": ask, "delta": delta,
                    "volatility": iv_pct, "openInterest": oi,
                }],
            }
        },
    }


# ── Test: full candidate evaluation pipeline ─────────────────────────────────

def test_evaluate_candidate_full_pipeline():
    # Fresh DB
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "int.db")

    import importlib
    from v2 import db as db_mod; importlib.reload(db_mod)
    from v2 import iv_rank as ivm; importlib.reload(ivm)
    from v2 import engine; importlib.reload(engine)

    candles = build_candles(260)
    mark_price = candles[-1]["close"]

    # Mock Schwab helpers globally
    with patch("v2.schwab_client.get_daily_candles", return_value=candles), \
         patch("v2.schwab_client.get_mark_price", return_value=mark_price), \
         patch("v2.schwab_client.get_put_chain_near_delta",
               return_value={
                   "underlying": mark_price, "expiry": "2026-05-15",
                   "strike": round(mark_price * 0.95, 0),
                   "bid": 2.80, "ask": 2.90, "mid": 2.85,
                   "iv": 32.0, "delta": 0.30,
                   "roi": round(2.85 / (mark_price * 0.95) * 100, 2),
                   "open_interest": 500,
               }), \
         patch("v2.iv_rank._fetch_daily_closes",
               return_value=[c["close"] for c in candles]), \
         patch("v2.iv_rank._fetch_current_iv", return_value=0.42), \
         patch("v2.fundamentals.get_fundamentals", return_value={
             "pe_positive": True, "beats_4q": True, "earnings_date": None,
         }):

        c = engine._evaluate_candidate({}, "TEST", vix=18.0,
                                        target_expiry="2026-05-15")
        assert c is not None, "Pipeline returned None"
        assert c["ticker"] == "TEST"
        assert c["iv_rank"] is not None
        assert c["score"] > 0
        assert c["tier"] in (None, 1, 2)
        # All data fields present
        for k in ("price", "strike", "delta", "mid", "roi", "kelly",
                  "dte", "expiry", "open_interest"):
            assert k in c, f"missing {k}"

    print("  [OK] _evaluate_candidate pipeline (Schwab mocked)")


# ── Test: dedupe prevents duplicate alerts ───────────────────────────────────

def test_dedupe_via_db():
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "dedupe.db")
    import importlib
    from v2 import db; importlib.reload(db)

    # First alert
    db.log_alert("AAPL", "ENTRY-LEAP", tier=2, score=65)
    assert db.was_alerted_recent("AAPL", "ENTRY-LEAP", hours=24)

    # Same ticker+tipo within 24h → dedupe should see it
    prev = db.last_alert_tier("AAPL", "ENTRY-LEAP", hours=24)
    assert prev == 2

    # Upgrade to T1 should still be flagged (prev_tier returned = 2)
    # Simulate new alert at T1 — manually insert & verify tier moves
    db.log_alert("AAPL", "ENTRY-LEAP", tier=1, score=82)
    assert db.last_alert_tier("AAPL", "ENTRY-LEAP", hours=24) == 1

    # MANAGE subtype dedupe
    db.log_alert("AMZN", "MANAGE", subtype="TAKE_PROFIT_50")
    assert db.was_alerted_recent("AMZN", "MANAGE", 24, subtype="TAKE_PROFIT_50")
    assert not db.was_alerted_recent("AMZN", "MANAGE", 24, subtype="ROLL_DECISION")

    print("  [OK] dedupe via alerts_log (ticker+tipo+subtype)")


# ── Test: hard filters block what they should ────────────────────────────────

def test_hard_filters_block():
    from v2 import filters

    closes = [100.0 + i * 0.1 for i in range(30)]
    today = _dt.date.today()
    exp   = (today + _dt.timedelta(days=30)).isoformat()

    # Pass case
    passed, flags = filters.passes_hard_filters(
        iv_rank=45, open_interest=500, bid=1.00, ask=1.02,
        strike=95.0, price=100.0, expiry=exp,
        earnings_date=None, closes=closes,
    )
    assert passed, f"Expected pass, got flags: {flags}"
    assert len(flags) == 0

    # Fail: low IV rank + low OI + wide spread
    passed, flags = filters.passes_hard_filters(
        iv_rank=10, open_interest=50, bid=1.00, ask=1.20,
        strike=95.0, price=100.0, expiry=exp,
        earnings_date=None, closes=closes,
    )
    assert not passed
    assert len(flags) >= 3  # at least IV + OI + spread flagged

    print("  [OK] passes_hard_filters blocks bad candidates, surfaces all flags")


# ── Test: conviction inputs with all fundamentals paths ──────────────────────

def test_conviction_full_path():
    from v2.scoring import ConvictionInputs, calc_conviction, apply_vix_modifier, classify_tier
    candles = build_candles(260)
    closes  = [c["close"] for c in candles]

    inp = ConvictionInputs(
        price=closes[-1], closes=closes, candles=candles,
        iv_rank=72.0, pe_positive=True, beats_4q=True,
        open_interest=800, spread_pct_of_mid=1.5,
    )
    score, details = calc_conviction(inp)
    # Should have non-zero components
    assert score > 0
    assert details["iv_rank"] == 72.0
    assert details["backtest_wr"] is not None

    # VIX modifier affects tier
    high_vix = apply_vix_modifier(score, 22.0)
    low_vix  = apply_vix_modifier(score, 12.0)
    assert high_vix >= score
    assert low_vix  <= score

    print("  [OK] conviction scoring end-to-end")


# ── Runner ───────────────────────────────────────────────────────────────────

def main():
    print("Running OTU v2.0 integration tests...")
    test_evaluate_candidate_full_pipeline()
    test_dedupe_via_db()
    test_hard_filters_block()
    test_conviction_full_path()
    print("\nAll integration tests passed [OK]")


if __name__ == "__main__":
    main()
