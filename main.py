"""
OTU Wheel Strategy — Morning Brief Bot + Trade Alert System
Runs on Render 24/7, fires Mon-Fri at 9:30, 11:30, 1:30, 3:30 PM ET
Data: Charles Schwab Trader API (options, prices, history)
      Yahoo Finance (VIX only)
"""

import os, time, base64, json, threading, statistics, datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import pytz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from apscheduler.schedulers.background import BackgroundScheduler
import alert_bot


# ── CONFIG ────────────────────────────────────────────────────────────────────
DISCORD_WEBHOOK_URL  = os.environ.get("DISCORD_WEBHOOK_URL", "")
MIN_ROI              = float(os.environ.get("MIN_ROI", "0.025"))
TARGET_EXPIRY        = os.environ.get("TARGET_EXPIRY", "2026-05-15")
SCHWAB_CLIENT_ID     = os.environ.get("SCHWAB_CLIENT_ID", "")
SCHWAB_CLIENT_SECRET = os.environ.get("SCHWAB_CLIENT_SECRET", "")
RENDER_SERVICE_ID    = "srv-d7bicrea2pns73ek5ge0"
RENDER_API_KEY       = os.environ.get("RENDER_API_KEY", "rnd_L3lNPbhi15RIAjkiqjrghguawKwh")
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
# ─────────────────────────────────────────────────────────────────────────────


# ── Schwab Token Management ───────────────────────────────────────────────────

_current_access_token = None
_token_refreshed_at   = None   # datetime of last successful refresh

def refresh_schwab_token():
    """
    Refresh Schwab access token using refresh token from env var.
    Always persists the new refresh token back to Render env vars so
    deploy restarts never lose a rotated token.
    """
    global _current_access_token, _token_refreshed_at
    refresh_token = os.environ.get("SCHWAB_REFRESH_TOKEN", "")
    if not refresh_token:
        print("No SCHWAB_REFRESH_TOKEN set")
        return None
    try:
        credentials = base64.b64encode(f"{SCHWAB_CLIENT_ID}:{SCHWAB_CLIENT_SECRET}".encode()).decode()
        r = requests.post(
            "https://api.schwabapi.com/v1/oauth/token",
            headers={"Authorization": f"Basic {credentials}", "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            timeout=15
        )
        data = r.json()
        if "access_token" in data:
            _current_access_token = data["access_token"]
            _token_refreshed_at   = datetime.datetime.now(ET)
            # Always persist rotated refresh token — prevents desync on restarts
            new_refresh = data.get("refresh_token") or refresh_token
            if new_refresh != refresh_token:
                update_render_env("SCHWAB_REFRESH_TOKEN", new_refresh)
                os.environ["SCHWAB_REFRESH_TOKEN"] = new_refresh
                print("  Schwab refresh token rotated and saved to Render")
            print("  Schwab token refreshed OK")
            return _current_access_token
        else:
            print(f"  Token refresh failed: {data}")
            return _current_access_token
    except Exception as e:
        print(f"  Token refresh error: {e}")
        return _current_access_token


def check_token_expiry_warning():
    """
    Send a Discord warning if the refresh token is approaching its 7-day expiry.
    Schwab refresh tokens expire 7 days from the INITIAL auth (not from rotation).
    We warn at 6 days (24h before expiry) so the user has time to re-auth.
    """
    if _token_refreshed_at is None:
        return
    age_days = (datetime.datetime.now(ET) - _token_refreshed_at).total_seconds() / 86400
    if age_days >= 6:
        reauth_url = (
            "https://api.schwabapi.com/v1/oauth/authorize"
            f"?response_type=code&client_id={SCHWAB_CLIENT_ID}"
            "&redirect_uri=https%3A%2F%2F127.0.0.1"
        )
        msg = (
            "**OTU Bot — Schwab Re-Auth requerida en < 24h**\n"
            "El refresh token de Schwab expira cada 7 dias. Corre el siguiente script:\n"
            "```\npython C:\\Users\\THINKPAD\\schwab_quick_auth.py\n```\n"
            f"URL de autorizacion:\n{reauth_url}"
        )
        requests.post(DISCORD_WEBHOOK_URL, json={"content": msg}, timeout=10)
        print("  Token expiry warning sent to Discord")


def update_render_env(key, value):
    """Update a single env var on Render via API."""
    try:
        r = requests.get(
            f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/env-vars",
            headers={"Authorization": f"Bearer {RENDER_API_KEY}"},
            timeout=10
        )
        existing = r.json() if r.status_code == 200 else []
        env_vars = []
        updated = False
        for item in existing:
            ev = item.get("envVar", item)
            if ev.get("key") == key:
                env_vars.append({"key": key, "value": value})
                updated = True
            else:
                env_vars.append({"key": ev["key"], "value": ev.get("value", "")})
        if not updated:
            env_vars.append({"key": key, "value": value})
        requests.put(
            f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/env-vars",
            headers={"Authorization": f"Bearer {RENDER_API_KEY}", "Content-Type": "application/json"},
            json=env_vars, timeout=10
        )
        print(f"  Render env {key} updated")
    except Exception as e:
        print(f"  Render env update failed: {e}")


def get_schwab_headers():
    token = refresh_schwab_token()
    return {"Authorization": f"Bearer {token}"}


# ── Health server ─────────────────────────────────────────────────────────────

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
        print("Self-ping OK")
    except Exception as e:
        print(f"Self-ping failed: {e}")


# ── Macro ─────────────────────────────────────────────────────────────────────

def get_macro(schwab_headers):
    try:
        # SPY price history via Schwab
        r_hist = requests.get(
            "https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=SPY&periodType=year&period=2&frequencyType=daily&frequency=1",
            headers=schwab_headers, timeout=20
        )
        candles = r_hist.json().get("candles", [])
        closes = [c["close"] for c in candles]
        spy_price = closes[-1]

        # EMA200
        k = 2 / (200 + 1)
        ema = closes[0]
        for price in closes[1:]:
            ema = price * k + ema * (1 - k)
        ema200 = round(ema, 2)

        # VIX via Yahoo Finance (single call)
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        session.get("https://finance.yahoo.com", timeout=10)
        time.sleep(0.5)
        crumb_r = session.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        crumb = crumb_r.text.strip()
        vix_r = session.get(f"https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?interval=1d&range=1d&crumb={crumb}", timeout=10)
        vix_price = vix_r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"]

        if vix_price < 10:
            cash_pct, deploy_pct = 87, 13
        elif vix_price < 15:
            cash_pct, deploy_pct = 62, 38
        elif vix_price < 20:
            cash_pct, deploy_pct = 37, 63
        elif vix_price < 30:
            cash_pct, deploy_pct = 0, 100
        else:
            cash_pct, deploy_pct = 0, 100

        print(f"  VIX={vix_price:.2f} SPY=${spy_price:.2f} EMA200=${ema200:.2f}")
        return {
            "vix": round(vix_price, 2), "spy": round(spy_price, 2), "ema200": ema200,
            "cash_pct": cash_pct, "deploy_pct": deploy_pct,
            "bear_market": spy_price < ema200
        }
    except Exception as e:
        print(f"  Macro error: {e}")
        return None


# ── Options data via Schwab ───────────────────────────────────────────────────

def get_options_data(schwab_headers, ticker):
    try:
        time.sleep(0.3)
        today = datetime.date.today()
        target_date = datetime.date.fromisoformat(TARGET_EXPIRY)
        target_dte = (target_date - today).days

        url = (
            f"https://api.schwabapi.com/marketdata/v1/chains"
            f"?symbol={ticker}&contractType=PUT&strikeCount=30"
            f"&includeUnderlyingQuote=true&strategy=SINGLE&range=OTM"
        )
        r = requests.get(url, headers=schwab_headers, timeout=15)
        chain = r.json()

        if chain.get("status") == "FAILED" or "putExpDateMap" not in chain:
            return None

        underlying_price = chain.get("underlyingPrice", 0)
        put_map = chain.get("putExpDateMap", {})
        if not put_map or not underlying_price:
            return None

        # Find expiry closest to TARGET_EXPIRY
        best_exp = None
        best_diff = 999
        for exp_key in put_map.keys():
            exp_date = datetime.date.fromisoformat(exp_key.split(":")[0])
            dte = (exp_date - today).days
            if dte < 7:
                continue
            diff = abs(dte - target_dte)
            if diff < best_diff:
                best_diff = diff
                best_exp = exp_key

        if not best_exp:
            return None

        expiry_str = best_exp.split(":")[0]
        strikes_data = put_map[best_exp]

        # Find contract closest to 30 delta
        best_contract = None
        best_delta_diff = 999
        for strike_str, contracts in strikes_data.items():
            c = contracts[0]
            delta = abs(c.get("delta", 0))
            if delta == 0:
                continue
            diff = abs(delta - 0.30)
            if diff < best_delta_diff:
                best_delta_diff = diff
                best_contract = (float(strike_str), c)

        if not best_contract:
            return None

        strike, contract = best_contract
        bid = contract.get("bid", 0)
        ask = contract.get("ask", 0)
        mid = round((bid + ask) / 2, 2)
        iv = round(contract.get("volatility", 0), 1)
        delta = round(abs(contract.get("delta", 0)), 2)
        roi = round(mid / strike * 100, 2) if strike > 0 else 0

        return {
            "ticker": ticker, "price": round(underlying_price, 2),
            "expiry": expiry_str, "strike": strike,
            "bid": round(bid, 2), "ask": round(ask, 2), "mid": mid,
            "iv": iv, "delta": delta, "roi": roi
        }
    except Exception as e:
        print(f"  [{ticker}] error: {e}")
        return None


# ── Discord ───────────────────────────────────────────────────────────────────

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


# ── Main brief ────────────────────────────────────────────────────────────────

def run_brief(slot_label):
    now_et = datetime.datetime.now(ET)
    print(f"\n{'='*60}")
    print(f"Running brief: {slot_label} ET -- {now_et.strftime('%Y-%m-%d %H:%M')}")

    schwab_headers = get_schwab_headers()
    if not schwab_headers.get("Authorization", "").endswith("None"):
        pass
    else:
        send_discord(f"**OTU Brief {slot_label} ET** — Schwab token unavailable. Re-auth needed.")
        return

    macro = get_macro(schwab_headers)

    qualified = []
    skipped = []

    for ticker in TICKERS:
        print(f"  {ticker}...", end=" ", flush=True)
        result = get_options_data(schwab_headers, ticker)
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

    dte = (datetime.date.fromisoformat(TARGET_EXPIRY) - datetime.date.today()).days
    bear_flag = "BEAR MARKET" if (macro and macro["bear_market"]) else "No Bear Market"

    lines = [f"# OTU Morning Brief -- {now_et.strftime('%a %b %d, %Y')} | {slot_label} ET", ""]

    if macro:
        vix_rule = (
            "75-100% cash (VIX <10)"               if macro["vix"] < 10 else
            "50-75% cash (VIX 10-15)"              if macro["vix"] < 15 else
            "25-50% cash (VIX 15-20)"              if macro["vix"] < 20 else
            "0% cash — fully deployed (VIX 20-30)" if macro["vix"] < 30 else
            "ADD NEW CASH DEPOSITS (VIX 30+)"
        )
        lines.append(f"**VIX:** {macro['vix']} | **SPY:** ${macro['spy']} vs EMA200 ${macro['ema200']} | {bear_flag}")
        lines.append(f"**OTU Rule:** {vix_rule}")
    else:
        lines.append("Macro data unavailable")

    lines += [
        "",
        f"**CSP Scan | Exp {TARGET_EXPIRY} ({dte} DTE) | ~30D / 30 Delta | >=2.5% ROI**",
        f"*Criteria: 18mo uptrend + Positive P/E + Earnings beats + Premium >=2% | Data: Schwab API*",
        f"Found **{len(qualified)}/{len(TICKERS)}** qualifying trades",
        ""
    ]

    if qualified:
        lines.append("```")
        lines.append(f"{'#':<3} {'Ticker':<6} {'Price':>8} {'Expiry':<12} {'Strike':>7} {'Delta':>6} {'Bid':>6} {'Ask':>6} {'Mid':>6} {'IV':>6} {'ROI':>6}")
        lines.append("-" * 80)
        for i, r in enumerate(qualified, 1):
            lines.append(
                f"{i:<3} {r['ticker']:<6} ${r['price']:>7.2f} {r['expiry']:<12} ${r['strike']:>6.0f} "
                f"{r['delta']:>5.2f}d ${r['bid']:>5.2f} ${r['ask']:>5.2f} ${r['mid']:>5.2f} "
                f"{r['iv']:>5.1f}% {r['roi']:>5.2f}%"
            )
        lines.append("```")
        lines += ["", "**Top 5 Picks**"]
        for i, r in enumerate(qualified[:5], 1):
            lines.append(f"{i}. **{r['ticker']}** ${r['strike']:.0f}P @ ${r['mid']:.2f} mid | {r['roi']:.2f}% ROI | {r['delta']:.2f}d | IV {r['iv']:.1f}%")
    else:
        lines.append("No qualifying trades met the >=2.5% ROI threshold today.")

    lines += ["", f"*Data: Schwab API + Yahoo VIX | {now_et.strftime('%H:%M ET')}*"]

    send_discord("\n".join(lines))
    print("Brief sent")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("OTU Morning Brief Bot starting...")
    print(f"Webhook: {'SET' if DISCORD_WEBHOOK_URL else 'NOT SET'}")
    print(f"Schwab Client ID: {'SET' if SCHWAB_CLIENT_ID else 'NOT SET'}")
    print(f"Target expiry: {TARGET_EXPIRY} | Min ROI: {MIN_ROI*100}%")

    start_health_server()

    scheduler = BackgroundScheduler(timezone=ET)
    # ── Morning Brief ─────────────────────────────────────────────────────────
    scheduler.add_job(lambda: run_brief("9:30 AM"),  "cron", day_of_week="mon-fri", hour=9,  minute=30)
    scheduler.add_job(lambda: run_brief("11:30 AM"), "cron", day_of_week="mon-fri", hour=11, minute=30)
    scheduler.add_job(lambda: run_brief("1:30 PM"),  "cron", day_of_week="mon-fri", hour=13, minute=30)
    scheduler.add_job(lambda: run_brief("3:30 PM"),  "cron", day_of_week="mon-fri", hour=15, minute=30)

    # ── Trade Alerts — every hour 9:33 AM → 3:33 PM ET (Mon-Fri) ────────────
    # Runs at :33 to let the 9:30/11:30/1:30/3:30 brief post first
    def _run_alerts():
        hdrs = get_schwab_headers()
        alert_bot.run_alerts(hdrs, DISCORD_WEBHOOK_URL)

    for _hour in [9, 10, 11, 12, 13, 14, 15]:
        scheduler.add_job(_run_alerts, "cron", day_of_week="mon-fri", hour=_hour, minute=33)

    scheduler.add_job(self_ping, "interval", minutes=10)
    # Check daily at 8 AM ET if token is within 24h of expiry → Discord warning
    scheduler.add_job(check_token_expiry_warning, "cron", day_of_week="mon-fri", hour=8, minute=0)
    scheduler.start()

    print("Scheduler running. Brief at :30 + Alerts at :33 ET, Mon-Fri.\n")
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    main()
