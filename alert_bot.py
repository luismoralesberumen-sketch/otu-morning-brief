"""
OTU Wheel Strategy — Trade Alert System
Runs at 9:30, 11:30, 1:30, 3:30 PM ET alongside the morning brief.
Conviction-scored technical alerts for bullish setups.
No duplicate alerts unless the tier improves.
"""

import math
import time
import requests
from datetime import datetime
import pytz

ET = pytz.timezone("America/New_York")


# ── Market Hours Detection ────────────────────────────────────────────────────

def _is_market_open() -> bool:
    """True if NYSE regular session is active (9:30–16:00 ET, Mon–Fri)."""
    now = datetime.now(ET)
    if now.weekday() >= 5:           # Saturday=5, Sunday=6
        return False
    open_  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_ = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return open_ <= now < close_


def _get_live_price(schwab_headers: dict, ticker: str):
    """
    Fetches the most current regular-session price from Schwab Quotes API.

    Priority:
      1. regularMarketLastPrice  — official last price of the regular session
                                   (today's close if market already closed,
                                    live price if market is open)
      2. closePrice              — today's settled close (populated post-close)
      3. mark                    — fallback mark price

    Never uses lastPrice alone — that can include after-hours trades.
    Returns float or None.
    """
    url = (
        "https://api.schwabapi.com/marketdata/v1/quotes"
        f"?symbols={ticker}&fields=quote"
    )
    try:
        r = requests.get(url, headers=schwab_headers, timeout=10)
        data = r.json()
        quote = data.get(ticker, {}).get("quote", {})
        price = (
            quote.get("regularMarketLastPrice")
            or quote.get("closePrice")
            or quote.get("mark")
        )
        return float(price) if price else None
    except Exception as e:
        print(f"    [ALERT] live price error {ticker}: {e}")
        return None

# ── Ticker Universe ───────────────────────────────────────────────────────────
# 61 morning brief tickers + additional from "Lista de seguimiento" watchlist

ALERT_TICKERS = [
    # ── Approved Wheel / Morning Brief (61) ──────────────────────────────
    "AMD", "VRT", "PLTR", "FUTU", "SHOP", "DELL", "CRDO", "ANET", "HOOD", "WDC",
    "CCJ", "UBER", "KTOS", "FTNT", "INOD", "CSCO", "IBIT", "META", "APP", "MSFT",
    "TSLA", "AXP", "AVGO", "GE", "JPM", "CLS", "TSM", "AAPL", "GOOGL", "STX",
    "AMZN", "MU", "NVDA", "ETHA", "SOFI", "NU", "CDE", "TIGR", "IREN", "AA",
    "ADI", "CCL", "HL", "AMAT", "LRCX", "APH", "EQT", "NEM", "CAT", "FCX",
    "RTX", "GLW", "COHR", "LMT", "MCD", "PGR", "ALL", "PG", "XOM", "T", "DAL",
    # ── Additional from watchlist (Open Positions / Focus / New / Acciones) ──
    "AXTI", "LITE", "SNDK", "ORCL", "CIFR", "INTC", "MSTR", "COIN", "COST",
    "NBIS", "NFLX", "SMCI", "WMT", "LAC", "MP", "DOCN", "FSLY",
]

# State: {ticker: {"tier": int, "score": int}}  — resets on restart (intentional)
_alert_state: dict = {}


# ── Technical Indicators ──────────────────────────────────────────────────────

def _calc_rsi(closes: list, period: int = 14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - (100.0 / (1.0 + rs))


def _calc_ema(data: list, period: int) -> list:
    if len(data) < period:
        return []
    k = 2.0 / (period + 1)
    ema = [sum(data[:period]) / period]
    for v in data[period:]:
        ema.append(v * k + ema[-1] * (1.0 - k))
    return ema


def _calc_macd(closes: list, fast: int = 12, slow: int = 26, signal: int = 9):
    if len(closes) < slow + signal:
        return None, None, None
    ef = _calc_ema(closes, fast)
    es = _calc_ema(closes, slow)
    offset = slow - fast
    macd_line = [f - s for f, s in zip(ef[offset:], es)]
    if len(macd_line) < signal:
        return None, None, None
    sig_line = _calc_ema(macd_line, signal)
    mv = macd_line[-1]
    sv = sig_line[-1]
    return round(mv, 4), round(sv, 4), round(mv - sv, 4)


def _calc_bb(closes: list, period: int = 20):
    if len(closes) < period:
        return None, None, None
    w = closes[-period:]
    sma = sum(w) / period
    std = math.sqrt(sum((x - sma) ** 2 for x in w) / period)
    return round(sma + 2 * std, 4), round(sma, 4), round(sma - 2 * std, 4)


def _calc_stochrsi(closes: list, rsi_p: int = 14, stoch_p: int = 14,
                   sk: int = 3, sd: int = 3):
    if len(closes) < rsi_p + stoch_p + sk + sd:
        return None, None
    rsi_series = [_calc_rsi(closes[:i + 1], rsi_p) for i in range(rsi_p, len(closes))]
    rsi_series = [r for r in rsi_series if r is not None]
    if len(rsi_series) < stoch_p:
        return None, None
    raw_k = []
    for i in range(stoch_p - 1, len(rsi_series)):
        w = rsi_series[i - stoch_p + 1: i + 1]
        lo, hi = min(w), max(w)
        raw_k.append(100.0 * (rsi_series[i] - lo) / (hi - lo) if hi != lo else 50.0)
    def sma_n(arr, n):
        return [sum(arr[j - n + 1: j + 1]) / n for j in range(n - 1, len(arr))]
    k_sm = sma_n(raw_k, sk)
    d_sm = sma_n(k_sm, sd)
    return (round(k_sm[-1], 2) if k_sm else None,
            round(d_sm[-1], 2) if d_sm else None)


def _vol_ratio(volumes: list, period: int = 20):
    if len(volumes) < period + 1:
        return None
    avg = sum(volumes[-period - 1:-1]) / period
    return round(volumes[-1] / avg, 2) if avg else None


def _high_52w(candles: list):
    w = candles[-252:] if len(candles) >= 252 else candles
    return max(c["high"] for c in w) if w else None


# ── Backtest Win Rate ─────────────────────────────────────────────────────────

def _backtest_wr(candles: list, fwd: int = 10, target: float = 0.03) -> int:
    """
    For each past day where RSI in [25,55] AND price within 8% above lower BB,
    check if price rose >= target% in the next `fwd` trading days.
    Returns integer 0-100 win rate.
    """
    closes_all = [c["close"] for c in candles]
    wins = total = 0
    for i in range(30, len(candles) - fwd):
        sub = closes_all[:i + 1]
        rsi_i = _calc_rsi(sub)
        _, _, lb = _calc_bb(sub)
        if rsi_i is None or lb is None or lb == 0:
            continue
        p = sub[-1]
        bb_dist = ((p - lb) / lb) * 100.0
        if 25.0 <= rsi_i <= 55.0 and 0.0 <= bb_dist <= 8.0:
            total += 1
            if closes_all[i + fwd] >= p * (1.0 + target):
                wins += 1
    if total < 3:
        return 50
    return round((wins / total) * 100)


# ── Conviction Score (0-100) ──────────────────────────────────────────────────

def calc_conviction(candles: list) -> tuple:
    """
    Returns (score: int, details: dict)

    Points breakdown:
      RSI(14)      20 pts  — bullish zone 30-60
      BB(20)       20 pts  — price near / below lower band
      StochRSI     15 pts  — oversold < 30
      MACD         15 pts  — bullish cross + positive histogram
      Volume       10 pts  — above-average participation
      Backtest WR  20 pts  — historical setup win rate
    """
    if len(candles) < 35:
        return 0, {}

    closes  = [c["close"]  for c in candles]
    volumes = [c["volume"] for c in candles]
    price   = closes[-1]
    score   = 0
    d: dict = {"price": round(price, 2)}

    # ── RSI (20 pts) ──────────────────────────────────────────────────────────
    rsi = _calc_rsi(closes)
    d["rsi"] = round(rsi, 1) if rsi is not None else None
    if rsi is not None:
        if   30 <= rsi <= 50:  score += 20   # ideal bounce from oversold
        elif 25 <= rsi <  30:  score += 15   # near oversold
        elif 50 <  rsi <= 60:  score += 13   # still bullish
        elif 60 <  rsi <= 70:  score +=  7   # overbought caution

    # ── Bollinger Bands (20 pts) ──────────────────────────────────────────────
    upper_bb, mid_bb, lower_bb = _calc_bb(closes)
    d["upper_bb"] = upper_bb; d["mid_bb"] = mid_bb; d["lower_bb"] = lower_bb
    if lower_bb and lower_bb > 0:
        bb_pct = ((price - lower_bb) / lower_bb) * 100.0
        d["bb_pct"] = round(bb_pct, 2)
        if   bb_pct <= 0:  score += 20  # at/below lower BB
        elif bb_pct <= 2:  score += 17
        elif bb_pct <= 5:  score += 12
        elif bb_pct <= 8:  score +=  7
        elif bb_pct <= 12: score +=  3
    else:
        d["bb_pct"] = None

    # ── StochRSI (15 pts) ─────────────────────────────────────────────────────
    stoch_k, stoch_d = _calc_stochrsi(closes)
    d["stochrsi_k"] = stoch_k; d["stochrsi_d"] = stoch_d
    if stoch_k is not None:
        if   stoch_k < 20: score += 15
        elif stoch_k < 30: score += 12
        elif stoch_k < 50 and stoch_d and stoch_k > stoch_d: score += 7  # bullish cross

    # ── MACD (15 pts) ─────────────────────────────────────────────────────────
    macd_v, macd_s, macd_h = _calc_macd(closes)
    d["macd"] = macd_v; d["macd_signal"] = macd_s; d["macd_hist"] = macd_h
    if macd_v is not None and macd_s is not None:
        if   macd_v > macd_s and macd_h and macd_h > 0: score += 15  # full bullish
        elif macd_v > macd_s:                            score += 10  # cross only
        elif macd_h and macd_h > 0 and macd_v < 0:      score +=  5  # early reversal

    # ── Volume (10 pts) ───────────────────────────────────────────────────────
    vr = _vol_ratio(volumes)
    d["vol_ratio"] = vr
    if vr is not None:
        if   vr >= 1.5: score += 10
        elif vr >= 1.2: score +=  7
        elif vr >= 0.9: score +=  4

    # ── Backtest Win Rate (20 pts) ────────────────────────────────────────────
    bt = _backtest_wr(candles)
    d["backtest_wr"] = bt
    score += int(20 * bt / 100)

    # Extras
    d["high_52w"] = _high_52w(candles)

    return min(score, 100), d


# ── Tier Classification ───────────────────────────────────────────────────────

def get_tier(score: int) -> tuple:
    if score >= 76: return 1, "LEAP Deep ITM ~79D (2027+)"
    if score >= 60: return 2, "Bull Call Spread / LEAP"
    if score >= 40: return 3, "CSP ~30D"
    if score >= 20: return 4, "CSP ~25D"
    return None, "No setup"


# ── Schwab Data ───────────────────────────────────────────────────────────────

def _get_history(schwab_headers: dict, ticker: str) -> list:
    """
    Returns 6 months of daily OHLCV candles from Schwab.

    Data source logic — always uses Schwab Quotes API for the last bar close:
      - Market OPEN   → regularMarketLastPrice = live real-time price
      - Market CLOSED → regularMarketLastPrice = today's official session close
      (pricehistory alone lags 1 day — Schwab updates daily bars with a delay)
    """
    url = (
        "https://api.schwabapi.com/marketdata/v1/pricehistory"
        f"?symbol={ticker}&periodType=month&period=6"
        "&frequencyType=daily&frequency=1&needExtendedHoursData=false"
    )
    try:
        r = requests.get(url, headers=schwab_headers, timeout=15)
        candles = r.json().get("candles", [])

        if candles:
            # Always fetch the most current regular-session price from Quotes API.
            # This fixes the 1-day lag in pricehistory regardless of market status.
            current_price = _get_live_price(schwab_headers, ticker)
            if current_price:
                last = dict(candles[-1])
                last["close"] = current_price
                last["high"]  = max(last.get("high", current_price), current_price)
                last["low"]   = min(last.get("low",  current_price), current_price)
                candles[-1]   = last

        return candles
    except Exception as e:
        print(f"    [ALERT] history error {ticker}: {e}")
        return []


# ── Discord Message Format ────────────────────────────────────────────────────

def _format_message(ticker: str, score: int, tier: int, tier_desc: str,
                    d: dict, prev_tier=None) -> str:
    now_et = datetime.now(ET)
    tier_arrow = f" | T{prev_tier} → T{tier} UPGRADE" if prev_tier and tier < prev_tier else ""

    price    = d.get("price", 0)
    high_52w = d.get("high_52w")
    pct_off  = (f" ({((price - high_52w) / high_52w * 100):.1f}% off 52w high)"
                if high_52w else "")

    rsi      = d.get("rsi", "—")
    bb_pct   = d.get("bb_pct")
    bb_str   = f"+{bb_pct:.1f}% above lower BB" if bb_pct is not None else "—"
    macd_h   = d.get("macd_hist")
    stoch_k  = d.get("stochrsi_k", "—")
    vol      = d.get("vol_ratio", "—")
    bt       = d.get("backtest_wr", "—")

    rsi_flag   = " ✅" if isinstance(rsi, float) and 30 <= rsi <= 60 else ""
    stoch_flag = " ✅ Oversold" if isinstance(stoch_k, float) and stoch_k < 30 else ""
    macd_dir   = "Bullish" if macd_h and macd_h > 0 else ("Bearish" if macd_h else "—")

    tier_emoji = {1: "🚀", 2: "📊", 3: "💰", 4: "🎯"}.get(tier, "🔔")

    msg = (
        f"## {tier_emoji} TRADE ALERT — **{ticker}**{tier_arrow}\n"
        f"```\n"
        f"Price:       ${price:.2f}{pct_off}\n"
        f"Score:       {score}/100   |   Tier {tier}: {tier_desc}\n"
        f"{'─' * 48}\n"
        f"RSI(14):     {rsi}{rsi_flag}\n"
        f"BB Dist:     {bb_str}\n"
        f"MACD Hist:   {macd_h if macd_h is not None else '—'}   ({macd_dir})\n"
        f"StochRSI K:  {stoch_k}{stoch_flag}\n"
        f"Volume:      {vol}x avg\n"
        f"Backtest WR: {bt}%  (last 6mo, 10-day fwd, +3% target)\n"
        f"{'─' * 48}\n"
        f"Strategy:    {tier_desc}\n"
        f"```\n"
        f"*{now_et.strftime('%I:%M %p ET')} — OTU Wheel Alert System*"
    )
    return msg


# ── Main Runner ───────────────────────────────────────────────────────────────

def run_alerts(schwab_headers: dict, webhook_url: str):
    """
    Called at each scheduled slot (9:30, 11:30, 1:30, 3:30 ET).
    Scans ALERT_TICKERS, computes conviction scores, sends Discord alerts for:
      - New setups (score >= 20, first detection)
      - Tier upgrades (tier number decreases, e.g., T3 -> T2)
    Skips if same tier already alerted.
    """
    now_et = datetime.now(ET)
    print(f"\n{'='*60}")
    print(f"[ALERTS] Scan start — {now_et.strftime('%H:%M ET')} | {len(ALERT_TICKERS)} tickers")

    alerts_sent = 0
    skipped     = 0
    errors      = 0

    for ticker in ALERT_TICKERS:
        try:
            time.sleep(0.35)  # ~2.8 tickers/sec — respect Schwab rate limit
            candles = _get_history(schwab_headers, ticker)

            if len(candles) < 35:
                print(f"  [ALERT] {ticker}: only {len(candles)} bars — skip")
                skipped += 1
                continue

            score, details = calc_conviction(candles)
            tier, tier_desc = get_tier(score)

            if tier is None:
                # Score < 20 — clear any stale state so if it recovers we re-alert
                if ticker in _alert_state:
                    del _alert_state[ticker]
                continue

            prev       = _alert_state.get(ticker)
            prev_tier  = prev["tier"] if prev else None
            should_send = (prev is None) or (tier < prev["tier"])

            if should_send:
                msg  = _format_message(ticker, score, tier, tier_desc, details, prev_tier)
                resp = requests.post(webhook_url, json={"content": msg}, timeout=10)
                if resp.status_code in (200, 204):
                    _alert_state[ticker] = {"tier": tier, "score": score}
                    label = (f"T{prev_tier}->T{tier} UPGRADE" if prev_tier else f"NEW T{tier}")
                    print(f"  [ALERT] SENT {ticker:6s} {label} score={score}")
                    alerts_sent += 1
                    time.sleep(0.5)  # Discord rate limit
                else:
                    print(f"  [ALERT] Discord error {resp.status_code} for {ticker}")
                    errors += 1
            else:
                print(f"  [ALERT] {ticker:6s} T{tier} score={score} — same tier, no resend")
                skipped += 1

        except Exception as e:
            print(f"  [ALERT] Exception on {ticker}: {e}")
            errors += 1

    print(f"[ALERTS] Done — {alerts_sent} sent | {skipped} skipped | {errors} errors")
