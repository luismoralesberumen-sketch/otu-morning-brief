"""
OTU Wheel v2.0 — Macro Economic Calendar

Tracks HIGH-impact US macro events that move vol:
    FOMC, CPI, NFP (Jobs), PPI

Source priority (fallback chain):
  1. TradingEconomics public calendar JSON (no API key needed)
  2. Hardcoded known dates for critical events (FOMC meetings)
  3. Manual upsert via db.upsert_macro_event

Cached weekly in macro_events table. Scheduler calls refresh_macro_calendar()
once per week (Sunday night). Filters query get_macro_events() during scans.
"""

from __future__ import annotations

import datetime as _dt
import re
from typing import Iterable

import requests

from . import db


_TE_URL = (
    "https://tradingeconomics.com/calendar"
    "?importance=3&country=united%20states&format=json"
)

# Keywords that mark an event as one of our watch types
_EVENT_PATTERNS = [
    ("FOMC", re.compile(r"\b(fomc|fed(eral)?\s+funds|interest\s+rate\s+decision|fed\s+chair)\b", re.I)),
    ("CPI",  re.compile(r"\b(cpi|consumer\s+price\s+index|inflation\s+rate)\b", re.I)),
    ("NFP",  re.compile(r"\b(non[-\s]?farm\s+payrolls|nfp|employment\s+change)\b", re.I)),
    ("PPI",  re.compile(r"\b(ppi|producer\s+price\s+index)\b", re.I)),
    ("JOBS", re.compile(r"\b(jobless\s+claims|unemployment\s+rate|jobs\s+report)\b", re.I)),
]


def _classify(event_title: str) -> str | None:
    for label, pattern in _EVENT_PATTERNS:
        if pattern.search(event_title or ""):
            return label
    return None


# ── Hardcoded fallback: FOMC meeting dates ───────────────────────────────────
# Known Fed meeting dates 2025-2027. Update annually.
_FOMC_DATES: tuple[str, ...] = (
    # 2025
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
    "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-10",
    # 2026
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
    # 2027 (tentative — Fed publishes forward schedule in June)
    "2027-01-27", "2027-03-17", "2027-04-28", "2027-06-16",
    "2027-07-28", "2027-09-15", "2027-10-27", "2027-12-08",
)


def _seed_known_fomc() -> int:
    """Insert known FOMC dates. Safe to call repeatedly (ON CONFLICT = update)."""
    count = 0
    for date in _FOMC_DATES:
        db.upsert_macro_event(date, "FOMC", impact="HIGH")
        count += 1
    return count


# ── Fetch from TradingEconomics (best-effort) ────────────────────────────────

def _fetch_te_calendar(days_ahead: int = 45) -> list[dict]:
    """
    Returns list of dicts {date, event_type}. Returns [] on any failure —
    we always have FOMC fallback seeded so this is an enrichment layer.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0 Safari/537.36"
        }
        r = requests.get(_TE_URL, headers=headers, timeout=15)
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception as e:
        print(f"  [MACRO] TE fetch failed: {e}")
        return []

    out: list[dict] = []
    today = _dt.date.today()
    cutoff = today + _dt.timedelta(days=days_ahead)
    for item in data:
        if not isinstance(item, dict):
            continue
        title = item.get("Event", "") or item.get("event", "")
        date_raw = item.get("Date", "") or item.get("date", "")
        if not title or not date_raw:
            continue
        # TE date formats vary; take first 10 chars if ISO-like
        try:
            ev_date = _dt.date.fromisoformat(date_raw[:10])
        except Exception:
            continue
        if ev_date < today or ev_date > cutoff:
            continue
        label = _classify(title)
        if label:
            out.append({"date": ev_date.isoformat(), "event_type": label})
    return out


# ── Public API ───────────────────────────────────────────────────────────────

def refresh_macro_calendar(days_ahead: int = 45) -> int:
    """
    Refresh macro_events table. Returns number of events upserted.
    Always seeds known FOMC dates, then enriches from TE calendar.
    """
    total = _seed_known_fomc()
    for ev in _fetch_te_calendar(days_ahead=days_ahead):
        db.upsert_macro_event(ev["date"], ev["event_type"], impact="HIGH")
        total += 1

    # Mark last refresh time
    db.kv_set("macro_last_refresh", _dt.datetime.utcnow().isoformat(timespec="seconds"))
    return total


def macro_is_stale(max_age_days: int = 7) -> bool:
    ts = db.kv_get("macro_last_refresh")
    if not ts:
        return True
    try:
        last = _dt.datetime.fromisoformat(ts)
    except Exception:
        return True
    return (_dt.datetime.utcnow() - last).days >= max_age_days


def upcoming_events(days_ahead: int = 5) -> list[dict]:
    rows = db.get_macro_events(days_ahead=days_ahead, only_high=True)
    return [{"date": r["date"], "event_type": r["event_type"]} for r in rows]


def has_event_within(hours: int = 24) -> list[dict]:
    rows = db.has_macro_event_within(hours=hours)
    return [{"date": r["date"], "event_type": r["event_type"]} for r in rows]
