"""
OTU Wheel Strategy — Morning Brief Bot
Runs on Render 24/7, fires briefs Mon-Fri at 9am, 11am, 2pm, 3:30pm ET
Sends formatted CSP opportunity table to Discord webhook.
"""

import os
import time
import threading
from datetime import datetime, date
from http.server import HTTPServer, BaseHTTPRequestHandler
import pytz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from apscheduler.schedulers.background import BackgroundScheduler


# ── CONFIG ───────────────────────────────────────────────────────────────────
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
MIN_ROI = float(os.environ.get("MIN_ROI", "0.025"))
TARGET_EXPIRY = os.environ.get("TARGET_EXPIRY", "2026-05-15")
ET = pytz.timezone("America/New_York")

TICKERS = [
    # Approved Wheel Stocks (FINAL OTU 04132026)
    "AMD", "VRT", "PLTR", "FUTU", "SHOP", "DELL", "CRDO", "ANET", "HOOD", "WDC",
    "CCJ", "UBER", "KTOS", "FTNT", "INOD", "CSCO", "IBIT", "META", "APP", "MSFT",
    "TSLA", "AXP", "AVGO", "GE", "JPM", "CLS", "TSM", "AAPL", "GOOGL", "STX",
    "AMZN", "MU", "NVDA", "ETHA", "SOFI", "NU", "CDE", "TIGR", "IREN", "AA",
    "ADI", "CCL", "HL", "AMAT", "LRCX", "APH", "EQT", "NEM", "CAT", "FCX",
    "RTX", "GLW", "COHR",
    # Safe Haven Stocks
    "LMT", "MCD", "PGR", "ALL", "PG", "XOM", "T",
    # Additional (hardcoded)
    "DAL",
]

# Browser-like headers to avoid Yahoo Finance blocking cloud IPs
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}
# ─────────────────────────────────────────────────────────────────────────────


def make_session():
    """Create a requests session with browser headers and retry logic."""
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_crumb(session):
    """Get Yahoo Finance crumb token required for API calls."""
    try:
        # Step 1: visit Yahoo Finance to get session cookie
        session.get("https://finance.yahoo.com", timeout=15)
        time.sleep(1)
        # Step 2: get crumb
        r = session.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        crumb = r.text.strip()
        if crumb and len(crumb) > 3:
            print(f"  Crumb obtained: {crumb[:8]}...")
            return crumb
        # Fallback: try alternative crumb endpoint
        r2 = session.get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        crumb = r2.text.strip()
        print(f"  Crumb (alt): {crumb[:8]}...")
        return crumb
    except Exception as e:
        print(f"  Crumb error: {e}")
        return None


# ── Health server ────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OTU Morning Brief Bot is running.")
    def log_message(self, format, *args):
        pass


def start_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Health server running on port {port}")


def self_ping():
    url = os.environ.get("RENDER_EXTERNAL_URL", "https://otu-morning-brief.onrender.com")
    try:
        requests.get(url, timeout=10)
        print(f"Self-ping OK")
    except Exception as e:
        print(f"Self-ping failed: {e}")


# ── Data fetching ────────────────────────────────────────────────────────────

def get_macro(session, crumb):
    """Fetch VIX and SPY via Yahoo Finance JSON API with crumb auth."""
    try:
        def fetch_price(symbol):
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d&crumb={crumb}"
            r = session.get(url, timeout=15)
            data = r.json()
            return data["chart"]["result"][0]["meta"]["regularMarketPrice"]

        vix_price = fetch_price("%5EVIX")
        spy_price = fetch_price("SPY")

        # SPY EMA200
        url200 = f"https://query1.finance.yahoo.com/v8/finance/chart/SPY?interval=1d&range=300d&crumb={crumb}"
        r200 = session.get(url200, timeout=15)
        closes = r200.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]

        # Calculate EMA200
        k = 2 / (200 + 1)
        ema = closes[0]
        for price in closes[1:]:
            ema = price * k + ema * (1 - k)
        ema200 = round(ema, 2)

        if vix_price < 10:
            cash_pct, deploy_pct = 87, 13    # 75-100% cash
        elif vix_price < 15:
            cash_pct, deploy_pct = 62, 38    # 50-75% cash
        elif vix_price < 20:
            cash_pct, deploy_pct = 37, 63    # 25-50% cash
        elif vix_price < 30:
            cash_pct, deploy_pct = 0, 100    # 0% cash, fully deployed
        else:
            cash_pct, deploy_pct = 0, 100    # 30+ = add new cash deposits

        print(f"  VIX={vix_price:.2f} SPY={spy_price:.2f} EMA200={ema200:.2f}")
        return {
            "vix": round(vix_price, 2),
            "spy": round(spy_price, 2),
            "ema200": ema200,
            "cash_pct": cash_pct,
            "deploy_pct": deploy_pct,
            "bear_market": spy_price < ema200
        }
    except Exception as e:
        print(f"Macro fetch error: {e}")
        return None


def get_options_data(session, crumb, ticker_sym):
    """Fetch CSP data via Yahoo Finance options API with crumb auth."""
    try:
        time.sleep(0.4)  # rate limit buffer

        # Get price
        url_quote = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_sym}?interval=1d&range=1d&crumb={crumb}"
        r_quote = session.get(url_quote, timeout=15)
        price = r_quote.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]

        # Get available expiry dates
        url_opts = f"https://query2.finance.yahoo.com/v7/finance/options/{ticker_sym}?crumb={crumb}"
        r_opts = session.get(url_opts, timeout=15)
        opts_json = r_opts.json()

        if "optionChain" not in opts_json:
            print(f"    No optionChain key for {ticker_sym}")
            return None

        result = opts_json["optionChain"].get("result", [])
        if not result:
            return None

        expiry_dates = result[0].get("expirationDates", [])
        today_dt = datetime.utcnow()

        candidates = [
            (abs((datetime.utcfromtimestamp(ts) - today_dt).days - 37), ts)
            for ts in expiry_dates
            if 20 <= (datetime.utcfromtimestamp(ts) - today_dt).days <= 55
        ]
        if not candidates:
            return None
        candidates.sort()
        target_ts = candidates[0][1]

        # Fetch options chain for that date
        url_chain = f"https://query2.finance.yahoo.com/v7/finance/options/{ticker_sym}?date={target_ts}&crumb={crumb}"
        r_chain = session.get(url_chain, timeout=15)
        chain_data = r_chain.json()["optionChain"]["result"][0]
        puts = chain_data.get("options", [{}])[0].get("puts", [])

        otm_puts = [p for p in puts if p.get("strike", 0) < price and p.get("bid", 0) > 0]
        if not otm_puts:
            return None

        ivs = [p.get("impliedVolatility", 0) for p in otm_puts if p.get("impliedVolatility", 0) > 0]
        avg_iv = sum(ivs) / len(ivs) if ivs else 0.5
        target_strike = price * (1 - 0.215 * avg_iv)
        best = min(otm_puts, key=lambda p: abs(p.get("strike", 0) - target_strike))

        bid = best.get("bid", 0)
        ask = best.get("ask", 0)
        mid = round((bid + ask) / 2, 2)
        strike = best.get("strike", 0)
        iv = round(best.get("impliedVolatility", 0) * 100, 1)
        roi = round(mid / strike * 100, 2) if strike > 0 else 0
        expiry_str = datetime.utcfromtimestamp(target_ts).strftime("%Y-%m-%d")

        return {
            "ticker": ticker_sym,
            "price": round(price, 2),
            "expiry": expiry_str,
            "strike": strike,
            "bid": round(bid, 2),
            "ask": round(ask, 2),
            "mid": mid,
            "iv": iv,
            "roi": roi
        }

    except Exception as e:
        print(f"  [{ticker_sym}] error: {e}")
        return None


def compute_dte(expiry):
    return (datetime.strptime(expiry, "%Y-%m-%d").date() - date.today()).days


# ── Discord ──────────────────────────────────────────────────────────────────

def send_discord(content):
    if not DISCORD_WEBHOOK_URL:
        print("No webhook URL set.")
        return
    chunks = [content[i:i+1990] for i in range(0, len(content), 1990)]
    for chunk in chunks:
        resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"Discord error {resp.status_code}: {resp.text}")
        time.sleep(0.5)


# ── Main brief ───────────────────────────────────────────────────────────────

def run_brief(slot_label):
    now_et = datetime.now(ET)
    print(f"\n{'='*60}")
    print(f"Running brief: {slot_label} ET -- {now_et.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    session = make_session()

    print("Getting crumb...")
    crumb = get_crumb(session)
    if not crumb:
        print("Failed to get crumb -- aborting")
        send_discord(f"# OTU Brief -- {slot_label} ET\n\nERROR: Could not authenticate with Yahoo Finance (crumb failed). Brief skipped.")
        return

    macro = get_macro(session, crumb)
    print(f"Macro: {macro}")

    qualified = []
    skipped = []

    for ticker in TICKERS:
        print(f"  Fetching {ticker}...", end=" ", flush=True)
        result = get_options_data(session, crumb, ticker)
        if result is None:
            print("no data")
            skipped.append(ticker)
            continue
        if result["roi"] >= MIN_ROI * 100:
            print(f"OK {result['roi']}%")
            qualified.append(result)
        else:
            print(f"skip {result['roi']}%")
            skipped.append(ticker)

    qualified.sort(key=lambda x: x["roi"], reverse=True)

    # Detect Yahoo Finance IP block: if ALL tickers returned no data, it's a block not a market issue
    no_data_count = len([t for t in TICKERS if t not in [r["ticker"] for r in qualified] and t not in skipped])
    total_no_data = len(TICKERS) - len(skipped) - len(qualified)
    blocked = (len(qualified) == 0 and total_no_data >= len(TICKERS) * 0.7)

    if blocked:
        send_discord(
            f"**OTU Brief -- {now_et.strftime('%a %b %d, %Y')} | {slot_label} ET**\n\n"
            f"⚠️ **Yahoo Finance is blocking Render's server IP — no options data available.**\n\n"
            f"Please run the brief manually from your computer:\n"
            f"```\npython C:\\Users\\THINKPAD\\run_brief_now.py\n```"
        )
        print("Yahoo Finance IP block detected — alert sent to Discord.")
        return

    dte = compute_dte(TARGET_EXPIRY)
    bear_flag = "BEAR MARKET" if (macro and macro["bear_market"]) else "No Bear Market"

    lines = [f"# OTU Morning Brief -- {now_et.strftime('%a %b %d, %Y')} | {slot_label} ET", ""]

    if macro:
        vix_rule = (
            "75-100% cash (VIX <10)" if macro['vix'] < 10 else
            "50-75% cash (VIX 10-15)" if macro['vix'] < 15 else
            "25-50% cash (VIX 15-20)" if macro['vix'] < 20 else
            "0% cash — fully deployed (VIX 20-30)" if macro['vix'] < 30 else
            "ADD NEW CASH DEPOSITS (VIX 30+)"
        )
        lines.append(f"**VIX:** {macro['vix']} | **SPY:** ${macro['spy']} vs EMA200 ${macro['ema200']} | {bear_flag}")
        lines.append(f"**OTU Rule:** {vix_rule}")
    else:
        lines.append("Macro data unavailable -- check Render logs")

    lines += ["", f"**CSP Scan | Exp {TARGET_EXPIRY} ({dte} DTE) | ~30D/30Delta | >=2.5% ROI**",
              f"*Criteria: 18mo uptrend + Positive P/E + Earnings beats + Premium >=2%*",
              f"Found **{len(qualified)}/{len(TICKERS)}** qualifying trades", ""]

    if qualified:
        lines.append("```")
        lines.append(f"{'#':<3} {'Ticker':<6} {'Price':>8} {'Expiry':<12} {'Strike':>7} {'Bid':>6} {'Ask':>6} {'Mid':>6} {'IV':>6} {'ROI':>6}")
        lines.append("-" * 74)
        for i, r in enumerate(qualified, 1):
            lines.append(f"{i:<3} {r['ticker']:<6} ${r['price']:>7.2f} {r.get('expiry', TARGET_EXPIRY):<12} ${r['strike']:>6.0f} "
                         f"${r['bid']:>5.2f} ${r['ask']:>5.2f} ${r['mid']:>5.2f} {r['iv']:>5.1f}% {r['roi']:>5.2f}%")
        lines.append("```")

        lines += ["", "**Top 5 Picks**"]
        medals = ["1.", "2.", "3.", "4.", "5."]
        for i, r in enumerate(qualified[:5]):
            lines.append(f"{medals[i]} **{r['ticker']}** ${r['strike']:.0f}P @ ${r['mid']:.2f} mid | {r['roi']:.2f}% ROI | IV {r['iv']:.1f}%")
    else:
        lines.append("No qualifying trades met the >=2.5% ROI threshold today.")

    lines += ["", f"*Data: Yahoo Finance | {now_et.strftime('%H:%M ET')}*"]

    send_discord("\n".join(lines))
    print(f"\nBrief sent")


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    print("OTU Morning Brief Bot starting...")
    print(f"Webhook configured: {'YES' if DISCORD_WEBHOOK_URL else 'NO'}")
    print(f"Target expiry: {TARGET_EXPIRY} | Min ROI: {MIN_ROI*100}%")

    start_health_server()

    scheduler = BackgroundScheduler(timezone=ET)
    scheduler.add_job(lambda: run_brief("9:30 AM"),  "cron", day_of_week="mon-fri", hour=9,  minute=30)
    scheduler.add_job(lambda: run_brief("11:30 AM"), "cron", day_of_week="mon-fri", hour=11, minute=30)
    scheduler.add_job(lambda: run_brief("1:30 PM"),  "cron", day_of_week="mon-fri", hour=13, minute=30)
    scheduler.add_job(lambda: run_brief("3:30 PM"),  "cron", day_of_week="mon-fri", hour=15, minute=30)
    scheduler.add_job(self_ping, "interval", minutes=10)
    scheduler.start()

    print("\nScheduler running. Waiting for next trigger...\n")
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Bot stopped.")


if __name__ == "__main__":
    main()
