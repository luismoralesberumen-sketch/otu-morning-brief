"""
OTU v2.0 — Unit tests for pure functions.

Run from repo root:
    python -m v2.tests.test_unit
"""

from __future__ import annotations

import os
import sys
import tempfile
import datetime as _dt


# Put repo root on path so `from v2 import ...` works when run directly
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def approx(a: float, b: float, tol: float = 0.01) -> bool:
    return abs(a - b) <= tol


# ── kelly.py ─────────────────────────────────────────────────────────────────

def test_kelly_fraction():
    from v2.kelly import kelly_fraction, kelly_score

    # 3% ROI, 70% WR, 15% max loss
    #   b = 3/15 = 0.2 ; p=0.7 ; q=0.3
    #   f* = (0.2*0.7 - 0.3) / 0.2 = (0.14 - 0.3)/0.2 = -0.8
    assert approx(kelly_fraction(3.0, 70.0, 15.0), -0.8)
    # Negative fraction → kelly_score returns 0
    assert kelly_score(3.0, 70.0, 15.0) == 0.0

    # 5% ROI, 80% WR, 10% max loss
    #   b = 5/10 = 0.5 ; p=0.8 ; q=0.2
    #   f* = (0.5*0.8 - 0.2) / 0.5 = (0.4 - 0.2)/0.5 = 0.4
    assert approx(kelly_fraction(5.0, 80.0, 10.0), 0.4)
    # score = 0.4 * 5 * 100 = 200
    assert approx(kelly_score(5.0, 80.0, 10.0), 200.0)

    # Zero ROI → zero score
    assert kelly_score(0.0, 90.0, 10.0) == 0.0
    # Zero max_loss → zero score (safety)
    assert kelly_score(3.0, 70.0, 0.0) == 0.0

    print("  [OK]kelly_fraction/score")


# ── scoring.py ───────────────────────────────────────────────────────────────

def test_scoring_components():
    from v2.scoring import (
        score_iv_rank, score_support, score_rsi_zone,
        score_fundamentals, score_option_liquidity, score_backtest,
        apply_vix_modifier, tier_thresholds, classify_tier,
    )

    # IV Rank bands
    assert score_iv_rank(None) == 0
    assert score_iv_rank(10)   == 0
    assert score_iv_rank(30)   == 10
    assert score_iv_rank(50)   == 18
    assert score_iv_rank(70)   == 25
    assert score_iv_rank(95)   == 25

    # Support: above EMA200 required
    assert score_support(100, lower_bb=95, ema50=98, ema200=105) == 0  # below 200
    assert score_support(100, lower_bb=100.1, ema50=98, ema200=95) == 20  # touching BB
    assert score_support(100, lower_bb=97,    ema50=98, ema200=95) == 12  # 3% above
    assert score_support(100, lower_bb=90,    ema50=98, ema200=95) == 8   # above 200 only

    # RSI zones
    assert score_rsi_zone(45) == 15
    assert score_rsi_zone(30) == 10
    assert score_rsi_zone(60) == 8
    assert score_rsi_zone(20) == 0

    # Fundamentals
    assert score_fundamentals(True, True)  == 15
    assert score_fundamentals(True, False) == 7
    assert score_fundamentals(False, False) == 0

    # Liquidity
    assert score_option_liquidity(500, 2.0) == 10
    assert score_option_liquidity(500, 6.0) == 5     # spread too wide → mid-tier
    assert score_option_liquidity(200, 1.0) == 5
    assert score_option_liquidity(50,  1.0) == 0

    # Backtest scoring: int(15 * wr/100)
    assert score_backtest(0)   == 0
    assert score_backtest(65)  == 9   # typical put-sell WR
    assert score_backtest(100) == 15

    # VIX modifier
    assert apply_vix_modifier(60, 25) == 70   # +10 for high VIX
    assert apply_vix_modifier(60, 18) == 60   # baseline
    assert apply_vix_modifier(60, 12) == 50   # -10 for low VIX
    assert apply_vix_modifier(95, 25) == 100  # capped
    assert apply_vix_modifier(5,  12) == 0    # floored

    # Tier thresholds
    assert tier_thresholds(25)   == (72, 55)
    assert tier_thresholds(18)   == (76, 60)
    assert tier_thresholds(12)   == (82, 68)
    assert tier_thresholds(None) == (76, 60)

    # Tier classification
    assert classify_tier(80, 18)[0] == 1
    assert classify_tier(65, 18)[0] == 2
    assert classify_tier(50, 18)[0] is None

    print("  [OK]scoring components + VIX modifier + tier thresholds")


def test_rsi_wilder():
    from v2.scoring import calc_rsi
    # Monotonic increasing → RSI → 100
    assert approx(calc_rsi([i for i in range(1, 50)]) or 0, 100.0, tol=0.1)
    # Flat → RSI = 100 (no losses)
    assert calc_rsi([10] * 30) == 100.0
    # Too short → None
    assert calc_rsi([1, 2, 3]) is None
    print("  [OK]calc_rsi (Wilder)")


# ── filters.py ───────────────────────────────────────────────────────────────

def test_filters():
    from v2.filters import (
        f_iv_rank, f_open_interest, f_spread,
        f_earnings_vs_expiry, stdev_20d,
    )

    assert f_iv_rank(35)[0] is True
    assert f_iv_rank(20)[0] is False
    assert f_iv_rank(None)[0] is False

    assert f_open_interest(150)[0] is True
    assert f_open_interest(50)[0]  is True   # min_oi now 50
    assert f_open_interest(10)[0]  is False

    # 3% spread OK
    assert f_spread(1.00, 1.03)[0] is True
    # 10% spread: mid=1.055, spread_pct=(0.10/1.055)*100=9.5% → passes now (max 10%)
    assert f_spread(1.00, 1.10)[0] is True
    # 25% spread too wide
    assert f_spread(1.00, 1.25)[0] is False

    # sigma: flat series → 0
    assert (stdev_20d([10] * 25) or 0) == 0.0

    # Earnings inside window, strike above safe line → reject
    closes = [100] * 19 + [100, 102, 98, 101, 99] + [100] * 10
    today = _dt.date.today()
    exp   = (today + _dt.timedelta(days=30)).isoformat()
    earn  = (today + _dt.timedelta(days=10)).isoformat()
    # With small sigma, any strike near price should fail
    passed, flag = f_earnings_vs_expiry(earn, exp, strike=100, price=100, closes=closes)
    assert passed is False
    # Earnings outside expiry window → pass regardless
    exp_short = (today + _dt.timedelta(days=5)).isoformat()
    earn_far  = (today + _dt.timedelta(days=40)).isoformat()
    assert f_earnings_vs_expiry(earn_far, exp_short, 100, 100, closes)[0] is True

    print("  [OK]filters (IV/OI/spread/earnings)")


# ── iv_rank.py pure helper ───────────────────────────────────────────────────

def test_iv_rank_formula():
    from v2.iv_rank import iv_rank_from_values
    assert iv_rank_from_values(0.3, 0.2, 0.5) == 100 * (0.3 - 0.2) / (0.5 - 0.2)
    assert iv_rank_from_values(0.2, 0.2, 0.5) == 0.0
    assert iv_rank_from_values(0.5, 0.2, 0.5) == 100.0
    # Degenerate range
    assert iv_rank_from_values(0.3, 0.3, 0.3) == 50.0
    # Current above max → clamped at 100
    assert iv_rank_from_values(0.6, 0.2, 0.5) == 100.0
    print("  [OK]iv_rank_from_values")


# ── db.py + anti-duplicate ──────────────────────────────────────────────────

def test_db_persistence_and_dedupe():
    # Use fresh temp DB
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "unit.db")
    # Force re-import so module-level globals reset
    import importlib
    from v2 import db as db_mod
    importlib.reload(db_mod)

    # alerts_log + dedupe
    db_mod.log_alert("AAPL", "ENTRY-LEAP", tier=2, score=68)
    assert db_mod.was_alerted_recent("AAPL", "ENTRY-LEAP", hours=1)
    assert not db_mod.was_alerted_recent("TSLA", "ENTRY-LEAP", hours=1)
    assert db_mod.last_alert_tier("AAPL", "ENTRY-LEAP", hours=1) == 2

    # Subtype-specific dedupe
    db_mod.log_alert("AMZN", "MANAGE", subtype="TAKE_PROFIT_50")
    assert db_mod.was_alerted_recent("AMZN", "MANAGE", subtype="TAKE_PROFIT_50")
    assert not db_mod.was_alerted_recent("AMZN", "MANAGE", subtype="ROLL_DECISION")

    # Positions
    db_mod.upsert_position("NVDA", 450, "2026-05-15", "PUT",
                            premium_init=500.0, contracts=2)
    pos = db_mod.get_open_positions()
    assert len(pos) == 1 and pos[0]["ticker"] == "NVDA"
    # Upsert same key → no duplicate
    db_mod.upsert_position("NVDA", 450, "2026-05-15", "PUT",
                            premium_init=520.0, contracts=2)
    assert len(db_mod.get_open_positions()) == 1

    # IV cache staleness
    assert db_mod.iv_cache_is_stale("NVDA")
    db_mod.set_iv_cache("NVDA", iv_min=0.2, iv_max=0.6, iv_current=0.45)
    assert not db_mod.iv_cache_is_stale("NVDA")

    # Macro events
    today = _dt.date.today().isoformat()
    db_mod.upsert_macro_event(today, "FOMC", impact="HIGH")
    within = db_mod.has_macro_event_within(hours=24)
    assert len(within) >= 1

    print("  [OK]db persistence + 24h dedupe window")


# ── macro_calendar classify ──────────────────────────────────────────────────

def test_macro_classify():
    from v2.macro_calendar import _classify
    assert _classify("Fed Interest Rate Decision")  == "FOMC"
    assert _classify("Consumer Price Index YoY")    == "CPI"
    assert _classify("Non-Farm Payrolls")           == "NFP"
    assert _classify("Producer Price Index MoM")    == "PPI"
    assert _classify("Initial Jobless Claims")      == "JOBS"
    assert _classify("Some irrelevant speech")      is None
    print("  [OK]macro classification")


# ── manage_module OSI parser ─────────────────────────────────────────────────

def test_osi_parse():
    from v2.manage_module import parse_osi, _is_monday_of_expiry_week
    r = parse_osi("AAPL  260515P00210000")
    assert r is not None
    assert r["ticker"] == "AAPL"
    assert r["expiry"] == "2026-05-15"
    assert r["type"]   == "PUT"
    assert approx(r["strike"], 210.0)

    # Call variant
    r2 = parse_osi("NVDA  270115C00500000")
    assert r2["type"] == "CALL" and r2["strike"] == 500.0

    # Invalid
    assert parse_osi("GARBAGE") is None
    print("  [OK]OSI parser + monday-of-expiry-week")


# ── Runner ───────────────────────────────────────────────────────────────────

def main():
    print("Running OTU v2.0 unit tests...")
    test_kelly_fraction()
    test_scoring_components()
    test_rsi_wilder()
    test_filters()
    test_iv_rank_formula()
    test_db_persistence_and_dedupe()
    test_macro_classify()
    test_osi_parse()
    print("\nAll unit tests passed [OK]")


if __name__ == "__main__":
    main()
