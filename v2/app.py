"""
OTU Wheel v2.0 — Application entry point

Replaces v1's main.py. Same Render deploy target. Schedules:
  ENTRY-CSP (Morning Brief): 09:45 and 15:00 ET, Mon-Fri
  ENTRY-LEAP (Trade Alerts): :33 past each hour 9:33-15:33 ET, Mon-Fri
  MANAGE:                    :33 past each hour 9:33-15:33 ET, Mon-Fri
  Macro calendar refresh:    Sunday 22:00 ET
  Token expiry check:        Daily 08:00 ET
"""

from __future__ import annotations

import os, base64, threading, time, datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

import pytz, requests
from apscheduler.schedulers.background import BackgroundScheduler

from . import engine, macro_calendar, db


ET = pytz.timezone("America/New_York")

DISCORD_WEBHOOK_URL  = os.environ.get("DISCORD_WEBHOOK_URL", "")
TARGET_EXPIRY        = os.environ.get("TARGET_EXPIRY", "2026-05-15")
SCHWAB_CLIENT_ID     = os.environ.get("SCHWAB_CLIENT_ID", "")
SCHWAB_CLIENT_SECRET = os.environ.get("SCHWAB_CLIENT_SECRET", "")
RENDER_SERVICE_ID    = os.environ.get("RENDER_SERVICE_ID", "srv-d7bicrea2pns73ek5ge0")
RENDER_API_KEY       = os.environ.get("RENDER_API_KEY", "")


# ── Token refresh (Render-persistent rotation) ───────────────────────────────

_token: Optional[str] = None
_token_refreshed_at: Optional[datetime.datetime] = None


def update_render_env(key: str, value: str) -> None:
    if not RENDER_API_KEY:
        return
    try:
        r = requests.get(
            f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/env-vars",
            headers={"Authorization": f"Bearer {RENDER_API_KEY}"}, timeout=10,
        )
        existing = r.json() if r.status_code == 200 else []
        env = []
        found = False
        for item in existing:
            ev = item.get("envVar", item)
            if ev.get("key") == key:
                env.append({"key": key, "value": value}); found = True
            else:
                env.append({"key": ev["key"], "value": ev.get("value", "")})
        if not found:
            env.append({"key": key, "value": value})
        requests.put(
            f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/env-vars",
            headers={"Authorization": f"Bearer {RENDER_API_KEY}",
                     "Content-Type": "application/json"},
            json=env, timeout=10,
        )
        print(f"  Render env {key} updated")
    except Exception as e:
        print(f"  Render env update failed: {e}")


def refresh_schwab_token() -> Optional[str]:
    global _token, _token_refreshed_at
    refresh_token = os.environ.get("SCHWAB_REFRESH_TOKEN", "")
    if not refresh_token:
        print("  No SCHWAB_REFRESH_TOKEN")
        return None
    try:
        creds = base64.b64encode(f"{SCHWAB_CLIENT_ID}:{SCHWAB_CLIENT_SECRET}".encode()).decode()
        r = requests.post(
            "https://api.schwabapi.com/v1/oauth/token",
            headers={"Authorization": f"Basic {creds}",
                     "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            timeout=15,
        )
        data = r.json()
        if "access_token" not in data:
            print(f"  Token refresh failed: {data}")
            return _token
        _token = data["access_token"]
        _token_refreshed_at = datetime.datetime.now(ET)
        new_refresh = data.get("refresh_token") or refresh_token
        if new_refresh != refresh_token:
            update_render_env("SCHWAB_REFRESH_TOKEN", new_refresh)
            os.environ["SCHWAB_REFRESH_TOKEN"] = new_refresh
            print("  Refresh token rotated → Render updated")
        print("  Schwab token refreshed OK")
        return _token
    except Exception as e:
        print(f"  Token refresh error: {e}")
        return _token


def get_schwab_headers() -> dict:
    return {"Authorization": f"Bearer {refresh_schwab_token()}"}


def check_token_expiry_warning() -> None:
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
            "Corre: `python C:\\Users\\THINKPAD\\schwab_quick_auth.py`\n"
            f"URL: {reauth_url}"
        )
        try:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": msg}, timeout=10)
        except Exception:
            pass


# ── Health server (Render requires an HTTP listener) ─────────────────────────

TRIGGER_TOKEN = os.environ.get("TRIGGER_TOKEN", "")


class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        u = urlparse(self.path)
        qs = parse_qs(u.query)
        if u.path == "/run":
            tok = (qs.get("token") or [""])[0]
            job = (qs.get("job")   or [""])[0]
            if not TRIGGER_TOKEN or tok != TRIGGER_TOKEN:
                self.send_response(401); self.end_headers()
                self.wfile.write(b"unauthorized"); return
            try:
                if job == "entry-csp":
                    threading.Thread(target=lambda: job_entry_csp("manual"), daemon=True).start()
                elif job == "entry-leap":
                    threading.Thread(target=job_entry_leap, daemon=True).start()
                elif job == "manage":
                    threading.Thread(target=job_manage, daemon=True).start()
                else:
                    self.send_response(400); self.end_headers()
                    self.wfile.write(b"unknown job"); return
                self.send_response(202); self.end_headers()
                self.wfile.write(f"{job} dispatched".encode()); return
            except Exception as e:
                self.send_response(500); self.end_headers()
                self.wfile.write(str(e).encode()); return
        self.send_response(200); self.end_headers()
        self.wfile.write(b"OTU Wheel v2.0 running.")
    def log_message(self, *_a, **_k): pass


def start_health_server():
    port = int(os.environ.get("PORT", 8080))
    srv = HTTPServer(("0.0.0.0", port), _Health)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"Health server on :{port}")


def self_ping():
    url = os.environ.get("RENDER_EXTERNAL_URL", "https://otu-morning-brief.onrender.com")
    try:
        requests.get(url, timeout=10)
    except Exception:
        pass


# ── Job wrappers ─────────────────────────────────────────────────────────────

def job_entry_csp(slot: str):
    engine.run_entry_csp(get_schwab_headers(), DISCORD_WEBHOOK_URL,
                         slot, target_expiry=TARGET_EXPIRY)

def job_entry_leap():
    engine.run_entry_leap(get_schwab_headers(), DISCORD_WEBHOOK_URL)

def job_manage():
    engine.run_manage(get_schwab_headers(), DISCORD_WEBHOOK_URL)

def job_refresh_macro():
    macro_calendar.refresh_macro_calendar()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("OTU Wheel v2.0 starting...")
    print(f"DB path: {db.db_path()}")
    print(f"Webhook: {'SET' if DISCORD_WEBHOOK_URL else 'NOT SET'}")
    print(f"Schwab: {'SET' if SCHWAB_CLIENT_ID else 'NOT SET'}")
    print(f"Target expiry: {TARGET_EXPIRY}")

    # Prime caches on boot
    try:
        if macro_calendar.macro_is_stale(max_age_days=7):
            n = macro_calendar.refresh_macro_calendar()
            print(f"Macro calendar: {n} events loaded")
    except Exception as e:
        print(f"Macro prime failed: {e}")

    start_health_server()

    sch = BackgroundScheduler(timezone=ET)

    # ENTRY-CSP: 09:45 and 15:00 Mon-Fri
    sch.add_job(lambda: job_entry_csp("9:45 AM"),  "cron", day_of_week="mon-fri", hour=9,  minute=45)
    sch.add_job(lambda: job_entry_csp("3:00 PM"),  "cron", day_of_week="mon-fri", hour=15, minute=0)

    # ENTRY-LEAP + MANAGE: every hour at :33, 9:33-15:33 Mon-Fri
    for h in range(9, 16):
        sch.add_job(job_entry_leap, "cron", day_of_week="mon-fri", hour=h, minute=33)
        # MANAGE runs 2 minutes later so it doesn't collide with LEAP scan
        sch.add_job(job_manage,     "cron", day_of_week="mon-fri", hour=h, minute=35)

    # Macro calendar refresh: Sunday 22:00 ET
    sch.add_job(job_refresh_macro, "cron", day_of_week="sun", hour=22, minute=0)

    # Token expiry warning: daily 08:00 ET
    sch.add_job(check_token_expiry_warning, "cron", day_of_week="mon-fri", hour=8, minute=0)

    # Keep Render instance warm
    sch.add_job(self_ping, "interval", minutes=10)

    sch.start()
    print("Scheduler v2.0 running. ENTRY-CSP :45/:00 | ENTRY-LEAP :33 | MANAGE :35")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        sch.shutdown()


if __name__ == "__main__":
    main()
