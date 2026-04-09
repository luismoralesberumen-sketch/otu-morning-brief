"""
OTU Wheel Strategy — Morning Brief Bot
Runs on Railway 24/7, fires briefs Mon-Fri at 9am, 11am, 2pm, 3:30pm ET
Sends formatted CSP opportunity table to Discord webhook.
"""

import os
import time
import traceback
from datetime import datetime, date
import pytz
import yfinance as yf
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

# ── CONFIG (set via Railway environment variables) ───────────────────────────
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
MIN_ROI = float(os.environ.get("MIN_ROI", "0.025"))          # 2.5%
TARGET_EXPIRY = os.environ.get("TARGET_EXPIRY", "2026-05-15")  # update when needed
ET = pytz.timezone("America/New_York")

TICKERS = [
    "AMD", "WDC", "AA", "FCX", "STX", "VRT", "DELL", "MU", "ADI", "AMAT",
    "GLW", "LRCX", "NEM", "CAT", "CCJ", "CLS", "TSM", "AVGO", "GE", "RTX",
    "CSCO", "LMT", "JPM", "EQT", "XOM", "T", "ALL", "GOOGL", "ANET", "NVDA"
]
# ─────────────────────────────────────────────────────────────────────────────


def get_macro():
    """Fetch VIX and SPY data."""
    try:
        vix = yf.Ticker("^VIX")
        vix_price = vix.fast_info["last_price"]

        spy = yf.Ticker("SPY")
        spy_price = spy.fast_info["last_price"]

        # SPY EMA200 via 200-day history
        hist = spy.history(period="300d")
        ema200 = hist["Close"].ewm(span=200, adjust=False).mean().iloc[-1]

        # OTU cash rule
        if vix_price < 20:
            cash_pct, deploy_pct = 0, 100
        elif vix_price < 25:
            cash_pct, deploy_pct = 20, 80
        elif vix_price < 30:
            cash_pct, deploy_pct = 40, 60
        else:
            cash_pct, deploy_pct = 60, 40

        bear = spy_price < ema200

        return {
            "vix": round(vix_price, 2),
            "spy": round(spy_price, 2),
            "ema200": round(ema200, 2),
            "cash_pct": cash_pct,
            "deploy_pct": deploy_pct,
            "bear_market": bear
        }
    except Exception as e:
        print(f"Macro fetch error: {e}")
        return None


def get_options_data(ticker_sym, expiry):
    """Fetch CSP data for a single ticker at the target expiry."""
    try:
        tk = yf.Ticker(ticker_sym)
        price = tk.fast_info["last_price"]

        # Check expiry is available
        available = tk.options
        if expiry not in available:
            # Find nearest available expiry 30-45 DTE
            target = datetime.strptime(expiry, "%Y-%m-%d").date()
            today = date.today()
            candidates = []
            for exp in available:
                exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
                dte = (exp_date - today).days
                if 25 <= dte <= 50:
                    candidates.append((abs(dte - 37), exp))
            if candidates:
                candidates.sort()
                expiry = candidates[0][1]
            else:
                return None

        chain = tk.option_chain(expiry)
        puts = chain.puts

        if puts.empty:
            return None

        # Filter OTM puts only (strike < price)
        puts = puts[puts["strike"] < price].copy()
        puts = puts[puts["bid"] > 0].copy()  # skip illiquid

        if puts.empty:
            return None

        # Calculate mid and ROI for each strike
        puts["mid"] = (puts["bid"] + puts["ask"]) / 2
        puts["roi"] = puts["mid"] / puts["strike"]
        puts["iv_pct"] = puts["impliedVolatility"] * 100

        # Find ~25 delta strike
        # OTM% ≈ 0.215 × IV (annualized)
        avg_iv = puts["impliedVolatility"].median()
        otm_pct = 0.215 * avg_iv
        target_strike = price * (1 - otm_pct)

        # Find nearest strike to target
        puts["strike_diff"] = abs(puts["strike"] - target_strike)
        best = puts.loc[puts["strike_diff"].idxmin()]

        return {
            "ticker": ticker_sym,
            "price": round(price, 2),
            "expiry": expiry,
            "strike": best["strike"],
            "bid": round(best["bid"], 2),
            "ask": round(best["ask"], 2),
            "mid": round(best["mid"], 2),
            "iv": round(best["iv_pct"], 1),
            "roi": round(best["roi"] * 100, 2),
            "oi": int(best["openInterest"]) if not str(best["openInterest"]) == "nan" else 0
        }

    except Exception as e:
        print(f"  [{ticker_sym}] error: {e}")
        return None


def compute_dte(expiry):
    exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
    return (exp_date - date.today()).days


def send_discord(content):
    """Send message to Discord webhook. Splits if >2000 chars."""
    if not DISCORD_WEBHOOK_URL:
        print("No webhook URL set.")
        return

    # Discord has 2000 char limit per message
    chunks = [content[i:i+1990] for i in range(0, len(content), 1990)]
    for chunk in chunks:
        resp = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": chunk},
            timeout=10
        )
        if resp.status_code not in (200, 204):
            print(f"Discord error {resp.status_code}: {resp.text}")
        time.sleep(0.5)


def run_brief(slot_label):
    """Main workflow — fetch all data and send brief."""
    now_et = datetime.now(ET)
    print(f"\n{'='*60}")
    print(f"Running brief: {slot_label} ET — {now_et.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    # ── Macro ────────────────────────────────────────────────────
    macro = get_macro()

    # ── Options scan ────────────────────────────────────────────
    qualified = []
    skipped = []

    for ticker in TICKERS:
        print(f"  Fetching {ticker}...", end=" ")
        result = get_options_data(ticker, TARGET_EXPIRY)
        if result is None:
            print("no data")
            skipped.append({"ticker": ticker, "reason": "no data"})
            continue

        if result["roi"] >= MIN_ROI * 100:
            print(f"✅ ROI {result['roi']}%")
            qualified.append(result)
        else:
            print(f"✗ ROI {result['roi']}%")
            skipped.append({"ticker": ticker, "roi": result["roi"], "reason": "below threshold"})

        time.sleep(0.3)  # be nice to Yahoo Finance

    # Sort by ROI descending
    qualified.sort(key=lambda x: x["roi"], reverse=True)

    # ── Format Discord message ───────────────────────────────────
    dte = compute_dte(TARGET_EXPIRY)
    bear_flag = "🐻 BEAR MARKET" if (macro and macro["bear_market"]) else "✅ No Bear Market"

    lines = []
    lines.append(f"# 📊 OTU Morning Brief — {now_et.strftime('%a %b %d, %Y')} | {slot_label} ET")
    lines.append("")

    if macro:
        lines.append(f"**VIX:** {macro['vix']} | **SPY:** ${macro['spy']} vs EMA200 ${macro['ema200']} | {bear_flag}")
        lines.append(f"**OTU Rule:** VIX {macro['vix']} → Deploy **{macro['deploy_pct']}%** / Hold **{macro['cash_pct']}%** cash")
    else:
        lines.append("⚠️ Macro data unavailable")

    lines.append("")
    lines.append(f"**CSP Scan | Exp {TARGET_EXPIRY} ({dte} DTE) | ≥2.5% ROI | ~25Δ**")
    lines.append(f"Found **{len(qualified)}/{len(TICKERS)}** qualifying trades")
    lines.append("")

    if qualified:
        lines.append("```")
        lines.append(f"{'#':<3} {'Ticker':<6} {'Price':>8} {'Strike':>7} {'Bid':>6} {'Ask':>6} {'Mid':>6} {'IV':>6} {'ROI':>6}")
        lines.append("-" * 62)
        for i, r in enumerate(qualified, 1):
            lines.append(
                f"{i:<3} {r['ticker']:<6} ${r['price']:>7.2f} "
                f"${r['strike']:>6.0f} ${r['bid']:>5.2f} ${r['ask']:>5.2f} "
                f"${r['mid']:>5.2f} {r['iv']:>5.1f}% {r['roi']:>5.2f}%"
            )
        lines.append("```")
    else:
        lines.append("⚠️ No qualifying trades found today.")

    # Top 5
    if qualified:
        lines.append("")
        lines.append("**🏆 Top 5 Picks**")
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for i, r in enumerate(qualified[:5]):
            lines.append(f"{medals[i]} **{r['ticker']}** ${r['strike']:.0f}P @ ${r['mid']:.2f} mid | {r['roi']:.2f}% ROI | IV {r['iv']:.1f}%")

    lines.append("")
    lines.append(f"*Data: Yahoo Finance live | {now_et.strftime('%H:%M ET')}*")

    message = "\n".join(lines)
    send_discord(message)
    print(f"\nBrief sent to Discord ✅")


def main():
    print("OTU Morning Brief Bot starting...")
    print(f"Schedules (ET): 9:00am | 11:00am | 2:00pm | 3:30pm | Mon-Fri")
    print(f"Target expiry: {TARGET_EXPIRY}")
    print(f"Tickers: {len(TICKERS)}")
    print(f"Webhook configured: {'YES' if DISCORD_WEBHOOK_URL else 'NO ⚠️'}")

    scheduler = BlockingScheduler(timezone=ET)

    scheduler.add_job(lambda: run_brief("9:00 AM"),  "cron", day_of_week="mon-fri", hour=9,  minute=0)
    scheduler.add_job(lambda: run_brief("11:00 AM"), "cron", day_of_week="mon-fri", hour=11, minute=0)
    scheduler.add_job(lambda: run_brief("2:00 PM"),  "cron", day_of_week="mon-fri", hour=14, minute=0)
    scheduler.add_job(lambda: run_brief("3:30 PM"),  "cron", day_of_week="mon-fri", hour=15, minute=30)

    print("\nScheduler running. Waiting for next trigger...\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped.")


if __name__ == "__main__":
    main()
