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


def calc_macd(closes: list[float], fast: int = 12, slow: int = 26,
              signal: int = 9) -> Optional[dict]:
    """
    Standard MACD: ema(fast) - ema(slow), signal = ema(macd, 9).
    Returns dict with last MACD line, signal line, histogram, and a
    boolean `ok` flag matching Pine v2.1 logic:
      ok = (histogram_rising) OR (macd_line > signal_line)
    Either condition True = momentum favorable for bullish/CSP setup.
    """
    if len(closes) < slow + signal + 1:
        return None
    ema_fast = calc_ema(closes, fast)
    ema_slow = calc_ema(closes, slow)
    if not ema_fast or not ema_slow:
        return None
    # Align lengths (ema_fast longer because shorter period started earlier)
    diff = len(ema_fast) - len(ema_slow)
    macd_line = [ema_fast[i + diff] - ema_slow[i] for i in range(len(ema_slow))]
    if len(macd_line) < signal + 1:
        return None
    signal_line = calc_ema(macd_line, signal)
    if not signal_line or len(signal_line) < 2:
        return None
    diff2 = len(macd_line) - len(signal_line)
    hist = [macd_line[i + diff2] - signal_line[i] for i in range(len(signal_line))]
    if len(hist) < 2:
        return None
    macd_now    = macd_line[-1]
    signal_now  = signal_line[-1]
    hist_now    = hist[-1]
    hist_prev   = hist[-2]
    rising      = hist_now > hist_prev
    bullish     = macd_now > signal_now
    return {
        "macd":   round(macd_now, 4),
        "signal": round(signal_now, 4),
        "hist":   round(hist_now, 4),
        "rising": rising,
        "bullish": bullish,
        "ok":     rising or bullish,   # Pine v2.1: macdRising OR macdBull
    }


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
    """
    Calibrated for low-VIX market reality: in regimes where VIX<20,
    individual ticker IVRs cluster in the 15-35 range. Hard binary at
    IVR=30 was killing baseline scores. Now IVR≥20 gets 5pts (some
    edge), IVR≥30 gets 10 (decent), IVR≥50 gets 18 (good), IVR≥70 gets 25 (premium).
    """
    if iv_rank is None:
        return 0
    if iv_rank >= 70: return 25
    if iv_rank >= 50: return 18
    if iv_rank >= 30: return 10
    if iv_rank >= 20: return 5
    return 0


def score_support(price: float, lower_bb: Optional[float],
                  ema50: Optional[float], ema200: Optional[float]) -> int:
    """
    Recalibrated: above EMA200 baseline reward up from 8→12. In a healthy
    uptrend, most tickers will sit above EMA200 without touching lower BB
    for weeks; treating that as "barely passing" was wrong.

    20 pts: touching lower BB + above EMA200 (textbook entry)
    14 pts: within 5% above lower BB + above EMA200 (close to support)
    12 pts: above EMA200 only (healthy uptrend baseline)
     0 pts: below EMA200 (trend broken)
    """
    if ema200 is None:
        return 0
    above_200 = price >= ema200
    if not above_200:
        return 0
    if lower_bb is not None and lower_bb > 0:
        bb_dist_pct = (price - lower_bb) / lower_bb * 100.0
        if bb_dist_pct <= 0:       return 20
        if bb_dist_pct <= 5:       return 14
    return 12


def score_rsi_zone(rsi: Optional[float], side: str = "PUT") -> int:
    """
    RSI preference depends on the trade direction:

    PUT (CSP — selling puts on pullbacks): we want names NOT extended up.
      30-50 : full credit (sweet spot)
      20-30 : oversold bonus
      50-55 : partial credit
      else  : 0

    CALL (LEAP buy / CC sell — directional bullish): we want momentum
    without exhaustion.
      50-65 : full credit (healthy uptrend)
      45-50 : partial (just turning up)
      >65   : 0 — extended, don't chase (hard gate blocks >68 in filters)
      <45   : 0 — momentum broken
    """
    if rsi is None:
        return 0
    if side == "CALL":
        if 50 <= rsi <= 65: return 15
        if 45 <= rsi < 50:  return 8
        return 0
    # default PUT
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
    side:        str = "PUT"        # "PUT" (CSP) or "CALL" (LEAP/CC)


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
    score += score_rsi_zone(rsi, side=inp.side)
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
    Recalibrated 2026-05-06 after observing 0/40 LEAP candidates in normal
    market conditions (VIX 17, uptrend). Lowered T1/T2 by ~4 pts per bucket
    so a "healthy uptrend with decent IVR" actually clears T2.

      VIX >= 21: T1=68, T2=52   (high vol — be permissive, more premium)
      VIX 15-21: T1=72, T2=56   (baseline — normal conditions)
      VIX <  15: T1=78, T2=64   (low vol — be selective)
    """
    if vix is None:
        return 72, 56
    if vix >= 21: return 68, 52
    if vix < 15:  return 78, 64
    return 72, 56


def classify_tier(score: int, vix: Optional[float]) -> tuple[Optional[int], str]:
    t1, t2 = tier_thresholds(vix)
    if score >= t1: return 1, "LEAP Deep ITM ~79D (2027+)"
    if score >= t2: return 2, "Bull Call Spread / LEAP"
    return None, "No setup"
