"""
OTU Wheel v2.0 — Conviction Score (100 pts) + VIX adaptive thresholds

Points breakdown:
    IV Rank (0-100)              25 pts  — vol-selling edge
    Support (BB + EMA50/200)     20 pts  — price location
    RSI zone                     15 pts  — momentum
    Fundamentals (P/E + beats)   15 pts  — company quality
    Options liquidity (OI+spread)10 pts  — execution quality
    Backtest win rate (252d)     15 pts  — historical edge

Removed from v1: StochRSI, MACD Histogram, relative volume.

VIX adaptive modifiers applied at scan time:
    VIX >= 21: score += 10 AND thresholds T1>=72, T2>=55
    VIX 15-20: baseline                 T1>=76, T2>=60
    VIX <  15: score -= 10 AND thresholds T1>=82, T2>=68

Each helper is pure (no I/O) so it's testable. The orchestration layer
(scheduler) pulls closes, IV rank, fundamentals, options chain and calls
calc_conviction().
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


# ── Indicator primitives ─────────────────────────────────────────────────────

def calc_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    """Wilder's smoothed RSI — identical to TradingView."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - (100.0 / (1.0 + rs))


def calc_ema(data: list[float], period: int) -> list[float]:
    if len(data) < period:
        return []
    k = 2.0 / (period + 1)
    ema = [sum(data[:period]) / period]
    for v in data[period:]:
        ema.append(v * k + ema[-1] * (1.0 - k))
    return ema


def calc_bb(closes: list[float], period: int = 20, mult: float = 2.0):
    if len(closes) < period:
        return None, None, None
    w = closes[-period:]
    sma = sum(w) / period
    std = math.sqrt(sum((x - sma) ** 2 for x in w) / period)
    return sma + mult * std, sma, sma - mult * std   # upper, mid, lower


# ── Backtest win rate (reused from v1, slightly tuned) ───────────────────────

def _sma_at(closes: list[float], idx: int, period: int = 50) -> Optional[float]:
    if idx < period:
        return None
    w = closes[idx - period: idx]
    return sum(w) / period


def backtest_win_rate(candles: list[dict], fwd: int = 30,
                       otm_pct: float = 0.05,
                       regime_aware: bool = True,
                       vix_series: Optional[list[float]] = None,
                       current_vix: Optional[float] = None) -> int:
    """
    Put-selling simulation with optional regime filtering.

    Base mechanic: for each rolling window, sell a put at price*(1-otm_pct)
    and check if price at day+fwd stays above that strike.

    Regime-aware mode (default): only counts past windows whose regime
    matches TODAY's regime:
      • Trend regime  : stock above/below its own 50d SMA
      • VIX bucket    : <15 / 15-20 / 20-30 / >30  (used only if vix_series
                        and current_vix are supplied, and aligned to candles)

    If too few regime-matched windows (<5) the function falls back to the
    full unsegmented WR so we don't starve the score.

    Returns int 0-100. Default fallback when history insufficient: 65.
    """
    closes = [c["close"] for c in candles]
    if len(closes) < fwd + 60:
        return 65

    # Determine TODAY's regime
    today_sma50 = _sma_at(closes, len(closes), 50)
    today_trend_up = (today_sma50 is not None) and (closes[-1] >= today_sma50)

    def _vix_bucket(v: Optional[float]) -> Optional[int]:
        if v is None: return None
        if v < 15:  return 0
        if v < 20:  return 1
        if v < 30:  return 2
        return 3
    today_vbucket = _vix_bucket(current_vix) if regime_aware else None

    have_vix_series = (
        regime_aware and vix_series is not None
        and len(vix_series) == len(closes)
        and today_vbucket is not None
    )

    wins = total = 0
    matched_wins = matched_total = 0
    step = 5
    for i in range(50, len(closes) - fwd, step):
        price  = closes[i]
        strike = price * (1.0 - otm_pct)
        future = closes[i + fwd]
        win = future >= strike
        total += 1
        if win: wins += 1

        if not regime_aware:
            continue

        # Regime match: trend + (optional) VIX bucket
        past_sma = _sma_at(closes, i, 50)
        if past_sma is None:
            continue
        past_trend_up = price >= past_sma
        if past_trend_up != today_trend_up:
            continue
        if have_vix_series:
            if _vix_bucket(vix_series[i]) != today_vbucket:
                continue

        matched_total += 1
        if win: matched_wins += 1

    if regime_aware and matched_total >= 5:
        return round((matched_wins / matched_total) * 100)
    if total < 5:
        return 65
    return round((wins / total) * 100)


# ── Component scoring (pure functions, 0-N points) ───────────────────────────

def score_iv_rank(iv_rank: Optional[float]) -> int:
    if iv_rank is None:
        return 0
    if iv_rank >= 70: return 25
    if iv_rank >= 50: return 18
    if iv_rank >= 30: return 10
    return 0


def score_support(price: float, lower_bb: Optional[float],
                  ema50: Optional[float], ema200: Optional[float]) -> int:
    """
    20 pts: touching lower BB + above EMA200
    12 pts: within 5% above lower BB + above EMA200
     8 pts: above EMA200 only
     0 pts: below EMA200
    """
    if ema200 is None:
        return 0
    above_200 = price >= ema200
    if not above_200:
        return 0
    if lower_bb is not None and lower_bb > 0:
        bb_dist_pct = (price - lower_bb) / lower_bb * 100.0
        if bb_dist_pct <= 0:       return 20
        if bb_dist_pct <= 5:       return 12
    return 8


def score_rsi_zone(rsi: Optional[float]) -> int:
    """
    Preference: RSI below 50 (oversold/neutral). We are selling premium on
    puts — we want names that are NOT extended to the upside.
      30-50 : full credit (sweet spot)
      20-30 : oversold bonus
      50-55 : partial credit
      >55 or <20 : no credit (over-extended or broken)
    """
    if rsi is None:
        return 0
    if 30 <= rsi <= 50: return 15
    if 20 <= rsi < 30:  return 12
    if 50 <  rsi <= 55: return 7
    return 0


def score_fundamentals(pe_positive: bool, beats_4q: bool) -> int:
    if pe_positive and beats_4q:  return 15
    if pe_positive or  beats_4q:  return 7
    return 0


def score_option_liquidity(open_interest: Optional[int],
                            spread_pct_of_mid: Optional[float]) -> int:
    if open_interest is None:
        return 0
    if open_interest >= 500 and (spread_pct_of_mid is not None and spread_pct_of_mid <= 3.0):
        return 10
    if open_interest >= 100:
        return 5
    return 0


def score_backtest(win_rate: int) -> int:
    return int(15 * max(0, min(100, win_rate)) / 100)


# ── Aggregator ───────────────────────────────────────────────────────────────

@dataclass
class ConvictionInputs:
    price:       float
    closes:      list[float]        # for RSI + BB + EMAs + backtest
    candles:     list[dict]         # for backtest (uses close)
    iv_rank:     Optional[float]
    pe_positive: bool
    beats_4q:    bool
    open_interest: Optional[int]
    spread_pct_of_mid: Optional[float]


def calc_conviction(inp: ConvictionInputs) -> tuple[int, dict]:
    """
    Main scoring entry point. Returns (score 0-100, details dict).
    Caller must already have IV Rank, fundamentals, and option-leg stats.
    """
    details: dict = {"price": round(inp.price, 2)}

    # Indicators
    rsi = calc_rsi(inp.closes)
    upper, mid, lower = calc_bb(inp.closes)
    ema50_list  = calc_ema(inp.closes, 50)
    ema200_list = calc_ema(inp.closes, 200)
    ema50  = ema50_list[-1]  if ema50_list  else None
    ema200 = ema200_list[-1] if ema200_list else None

    details["rsi"]      = round(rsi, 2) if rsi is not None else None
    details["lower_bb"] = round(lower, 2) if lower is not None else None
    details["mid_bb"]   = round(mid,   2) if mid   is not None else None
    details["upper_bb"] = round(upper, 2) if upper is not None else None
    details["ema50"]    = round(ema50,  2) if ema50  is not None else None
    details["ema200"]   = round(ema200, 2) if ema200 is not None else None
    details["iv_rank"]  = round(inp.iv_rank, 1) if inp.iv_rank is not None else None
    details["pe_positive"] = inp.pe_positive
    details["beats_4q"]    = inp.beats_4q
    details["open_interest"] = inp.open_interest
    details["spread_pct"]    = inp.spread_pct_of_mid

    wr = backtest_win_rate(inp.candles) if len(inp.candles) >= 50 else 50
    details["backtest_wr"] = wr

    # Score components
    score = 0
    score += score_iv_rank(inp.iv_rank)
    score += score_support(inp.price, lower, ema50, ema200)
    score += score_rsi_zone(rsi)
    score += score_fundamentals(inp.pe_positive, inp.beats_4q)
    score += score_option_liquidity(inp.open_interest, inp.spread_pct_of_mid)
    score += score_backtest(wr)

    return min(score, 100), details


# ── VIX-adaptive modifier + dynamic tier thresholds ──────────────────────────

def apply_vix_modifier(base_score: int, vix: Optional[float]) -> int:
    if vix is None:
        return base_score
    if vix >= 21:
        return min(base_score + 10, 100)
    if vix < 15:
        return max(base_score - 10, 0)
    return base_score


def tier_thresholds(vix: Optional[float]) -> tuple[int, int]:
    """
    Returns (T1_threshold, T2_threshold) based on VIX regime.
      VIX >= 21: 72 / 55
      VIX 15-20: 76 / 60 (baseline)
      VIX <  15: 82 / 68
    """
    if vix is None:
        return 76, 60
    if vix >= 21: return 72, 55
    if vix < 15:  return 82, 68
    return 76, 60


def classify_tier(score: int, vix: Optional[float]) -> tuple[Optional[int], str]:
    t1, t2 = tier_thresholds(vix)
    if score >= t1: return 1, "LEAP Deep ITM ~79D (2027+)"
    if score >= t2: return 2, "Bull Call Spread / LEAP"
    return None, "No setup"
