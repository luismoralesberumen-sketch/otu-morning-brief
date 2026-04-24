"""
OTU Wheel v2.0 — SQLite persistence layer

All state that must survive Render restarts lives here:
  • alerts_log    — anti-duplicate window (24h) + alert history
  • positions     — tracked open positions from Schwab
  • iv_cache      — 52-week IV min/max per ticker (refreshed daily)
  • macro_events  — FOMC/CPI/NFP/PPI/Jobs calendar (refreshed weekly)

DB path resolution:
  1. env DB_PATH (explicit override)
  2. /data/otu.db (Render disk mount)
  3. ./otu.db     (local dev fallback)
"""

from __future__ import annotations

import os
import sqlite3
import threading
import datetime as _dt
from typing import Optional


# ── Connection handling ──────────────────────────────────────────────────────

_DB_LOCK  = threading.Lock()
_CONN: Optional[sqlite3.Connection] = None
_DB_PATH: Optional[str] = None


def _resolve_path() -> str:
    override = os.environ.get("DB_PATH")
    if override:
        return override
    # Render disk mount
    if os.path.isdir("/data"):
        return "/data/otu.db"
    # Local dev fallback
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "otu.db")


def get_conn() -> sqlite3.Connection:
    """Return the singleton connection, initializing schema on first call."""
    global _CONN, _DB_PATH
    if _CONN is not None:
        return _CONN
    with _DB_LOCK:
        if _CONN is not None:
            return _CONN
        _DB_PATH = _resolve_path()
        os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
        conn = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _init_schema(conn)
        _migrate_schema(conn)
        _CONN = conn
        return _CONN


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Idempotent additive migrations. Never drops or renames."""
    cur = conn.cursor()
    # alerts_log: add per-contract snapshot columns for outcome evaluation
    existing = {r["name"] for r in cur.execute("PRAGMA table_info(alerts_log)").fetchall()}
    add = []
    if "side"            not in existing: add.append("side TEXT")              # PUT / CALL
    if "strike"          not in existing: add.append("strike REAL")
    if "expiry"          not in existing: add.append("expiry TEXT")            # YYYY-MM-DD
    if "mid_at_alert"    not in existing: add.append("mid_at_alert REAL")      # premium received proxy
    if "delta_at_alert"  not in existing: add.append("delta_at_alert REAL")
    if "iv_rank_at_alert" not in existing: add.append("iv_rank_at_alert REAL")
    if "roi_at_alert"    not in existing: add.append("roi_at_alert REAL")
    if "price_at_alert"  not in existing: add.append("price_at_alert REAL")    # underlying at alert
    for col in add:
        cur.execute(f"ALTER TABLE alerts_log ADD COLUMN {col}")

    cur.executescript("""
    -- Outcome evaluation per alert at T+N checkpoints
    CREATE TABLE IF NOT EXISTS alert_outcomes (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_id        INTEGER NOT NULL,
        eval_date       TEXT    NOT NULL,        -- YYYY-MM-DD
        days_since      INTEGER NOT NULL,        -- 7 / 14 / 21 / 30 / at_expiry
        price_at_eval   REAL,
        pct_to_strike   REAL,                    -- (price - strike) / strike * 100
        outcome_class   TEXT,                    -- OTM_SAFE | ITM_TOUCH | BREACHED | EXPIRED_OTM | EXPIRED_ITM
        pnl_est_pct     REAL,                    -- estimated P/L% on premium basis
        notes           TEXT,
        UNIQUE(alert_id, days_since),
        FOREIGN KEY(alert_id) REFERENCES alerts_log(id)
    );
    CREATE INDEX IF NOT EXISTS ix_outcomes_alert ON alert_outcomes(alert_id);
    CREATE INDEX IF NOT EXISTS ix_outcomes_eval  ON alert_outcomes(eval_date);

    -- Shadow-mode v2.5 probability logging (runs alongside; does not gate decisions)
    CREATE TABLE IF NOT EXISTS probability_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_id    INTEGER,
        ticker      TEXT    NOT NULL,
        side        TEXT    NOT NULL,
        created_at  TEXT    NOT NULL,
        regime      TEXT,                         -- e.g. "VIX15-20/above50"
        p_tech      REAL,
        p_macro     REAL,
        p_fund      REAL,
        p_flow      REAL,
        p_sent      REAL,
        p_total     REAL,
        ev_pct      REAL,
        kelly_adj   REAL,
        FOREIGN KEY(alert_id) REFERENCES alerts_log(id)
    );
    CREATE INDEX IF NOT EXISTS ix_plog_ticker ON probability_log(ticker, created_at);
    """)
    conn.commit()


def _init_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS alerts_log (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker       TEXT    NOT NULL,
        tier         INTEGER,
        score        INTEGER,
        timestamp    TEXT    NOT NULL,          -- ISO8601 UTC
        tipo         TEXT    NOT NULL,          -- ENTRY-CSP | ENTRY-LEAP | MANAGE
        subtype      TEXT,                      -- TAKE_PROFIT_50 / ROLL_DECISION / etc.
        filled_bool  INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS ix_alerts_ticker_tipo_ts
        ON alerts_log(ticker, tipo, timestamp);
    CREATE INDEX IF NOT EXISTS ix_alerts_subtype_ts
        ON alerts_log(ticker, subtype, timestamp);

    CREATE TABLE IF NOT EXISTS positions (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker        TEXT    NOT NULL,
        strike        REAL    NOT NULL,
        expiry        TEXT    NOT NULL,         -- YYYY-MM-DD
        type          TEXT    NOT NULL,         -- PUT / CALL
        contracts     INTEGER DEFAULT 1,
        premium_init  REAL    NOT NULL,         -- opening credit in dollars
        opened_at     TEXT    NOT NULL,
        status        TEXT    NOT NULL DEFAULT 'OPEN',  -- OPEN / CLOSED / ASSIGNED / ROLLED
        closed_at     TEXT,
        notes         TEXT,
        UNIQUE(ticker, strike, expiry, type)
    );
    CREATE INDEX IF NOT EXISTS ix_positions_status ON positions(status);

    CREATE TABLE IF NOT EXISTS iv_cache (
        ticker       TEXT PRIMARY KEY,
        iv_min_52w   REAL,
        iv_max_52w   REAL,
        iv_current   REAL,
        updated_at   TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS macro_events (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        date       TEXT NOT NULL,              -- YYYY-MM-DD
        event_type TEXT NOT NULL,              -- FOMC | CPI | NFP | PPI | JOBS
        impact     TEXT NOT NULL DEFAULT 'HIGH',
        UNIQUE(date, event_type)
    );
    CREATE INDEX IF NOT EXISTS ix_macro_date ON macro_events(date);

    CREATE TABLE IF NOT EXISTS kv_state (
        k TEXT PRIMARY KEY,
        v TEXT,
        updated_at TEXT NOT NULL
    );
    """)
    conn.commit()


# ── alerts_log ───────────────────────────────────────────────────────────────

def log_alert(ticker: str, tipo: str, tier: Optional[int] = None,
              score: Optional[int] = None, subtype: Optional[str] = None,
              filled_bool: bool = False,
              # Per-contract snapshot (Camino B — outcome evaluation)
              side: Optional[str] = None,
              strike: Optional[float] = None,
              expiry: Optional[str] = None,
              mid_at_alert: Optional[float] = None,
              delta_at_alert: Optional[float] = None,
              iv_rank_at_alert: Optional[float] = None,
              roi_at_alert: Optional[float] = None,
              price_at_alert: Optional[float] = None) -> int:
    """Insert an alert record. Returns the row id.

    The extended fields (strike/expiry/mid/delta/ivr/roi/price) are used by the
    outcome evaluator to measure real performance at T+7/14/21/30. All are
    optional — legacy callers continue to work.
    """
    conn = get_conn()
    ts = _dt.datetime.utcnow().isoformat(timespec="seconds")
    with _DB_LOCK:
        cur = conn.execute(
            "INSERT INTO alerts_log (ticker, tier, score, timestamp, tipo, subtype, filled_bool, "
            "  side, strike, expiry, mid_at_alert, delta_at_alert, iv_rank_at_alert, "
            "  roi_at_alert, price_at_alert) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ticker, tier, score, ts, tipo, subtype, 1 if filled_bool else 0,
             side, strike, expiry, mid_at_alert, delta_at_alert, iv_rank_at_alert,
             roi_at_alert, price_at_alert)
        )
        conn.commit()
        return cur.lastrowid


# ── alert_outcomes (Camino B) ────────────────────────────────────────────────

def upsert_outcome(alert_id: int, days_since: int, eval_date: str,
                    price_at_eval: Optional[float], pct_to_strike: Optional[float],
                    outcome_class: str, pnl_est_pct: Optional[float] = None,
                    notes: Optional[str] = None) -> None:
    conn = get_conn()
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO alert_outcomes (alert_id, eval_date, days_since, price_at_eval, "
            "  pct_to_strike, outcome_class, pnl_est_pct, notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(alert_id, days_since) DO UPDATE SET "
            "  eval_date=excluded.eval_date, price_at_eval=excluded.price_at_eval, "
            "  pct_to_strike=excluded.pct_to_strike, outcome_class=excluded.outcome_class, "
            "  pnl_est_pct=excluded.pnl_est_pct, notes=excluded.notes",
            (alert_id, eval_date, days_since, price_at_eval, pct_to_strike,
             outcome_class, pnl_est_pct, notes)
        )
        conn.commit()


def alerts_pending_evaluation(days_since_min: int = 7) -> list[sqlite3.Row]:
    """
    Return alerts that are >= days_since_min old and lack an outcome row for
    the corresponding checkpoint. Caller picks the checkpoint per alert.
    Only ENTRY-* alerts with strike+expiry populated are returned.
    """
    conn = get_conn()
    cutoff = (_dt.datetime.utcnow() - _dt.timedelta(days=days_since_min)).isoformat(timespec="seconds")
    return conn.execute(
        "SELECT a.* FROM alerts_log a "
        "WHERE a.tipo LIKE 'ENTRY-%' "
        "  AND a.strike IS NOT NULL AND a.expiry IS NOT NULL "
        "  AND a.timestamp <= ? "
        "ORDER BY a.timestamp ASC",
        (cutoff,)
    ).fetchall()


def outcomes_for_alert(alert_id: int) -> list[sqlite3.Row]:
    conn = get_conn()
    return conn.execute(
        "SELECT * FROM alert_outcomes WHERE alert_id=? ORDER BY days_since ASC",
        (alert_id,)
    ).fetchall()


# ── probability_log (v2.5 shadow-mode) ───────────────────────────────────────

def log_probability(ticker: str, side: str, alert_id: Optional[int] = None,
                     regime: Optional[str] = None,
                     p_tech: Optional[float] = None, p_macro: Optional[float] = None,
                     p_fund: Optional[float] = None, p_flow: Optional[float] = None,
                     p_sent: Optional[float] = None, p_total: Optional[float] = None,
                     ev_pct: Optional[float] = None, kelly_adj: Optional[float] = None) -> int:
    conn = get_conn()
    ts = _dt.datetime.utcnow().isoformat(timespec="seconds")
    with _DB_LOCK:
        cur = conn.execute(
            "INSERT INTO probability_log (alert_id, ticker, side, created_at, regime, "
            "  p_tech, p_macro, p_fund, p_flow, p_sent, p_total, ev_pct, kelly_adj) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (alert_id, ticker, side, ts, regime, p_tech, p_macro, p_fund, p_flow,
             p_sent, p_total, ev_pct, kelly_adj)
        )
        conn.commit()
        return cur.lastrowid


def was_alerted_recent(ticker: str, tipo: str, hours: int = 24,
                        subtype: Optional[str] = None) -> bool:
    """
    True if we've logged an alert for (ticker, tipo[, subtype]) in the last `hours`.
    Replaces the in-memory _alert_state dict from v1.
    """
    conn = get_conn()
    cutoff = (_dt.datetime.utcnow() - _dt.timedelta(hours=hours)).isoformat(timespec="seconds")
    if subtype is None:
        row = conn.execute(
            "SELECT 1 FROM alerts_log WHERE ticker=? AND tipo=? AND timestamp>=? LIMIT 1",
            (ticker, tipo, cutoff)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT 1 FROM alerts_log WHERE ticker=? AND tipo=? AND subtype=? AND timestamp>=? LIMIT 1",
            (ticker, tipo, subtype, cutoff)
        ).fetchone()
    return row is not None


def last_alert_tier(ticker: str, tipo: str, hours: int = 24) -> Optional[int]:
    """Most-recent tier for this ticker+tipo within window. Used for upgrade detection."""
    conn = get_conn()
    cutoff = (_dt.datetime.utcnow() - _dt.timedelta(hours=hours)).isoformat(timespec="seconds")
    row = conn.execute(
        "SELECT tier FROM alerts_log WHERE ticker=? AND tipo=? AND timestamp>=? "
        "ORDER BY id DESC LIMIT 1",
        (ticker, tipo, cutoff)
    ).fetchone()
    return row["tier"] if row and row["tier"] is not None else None


# ── positions ────────────────────────────────────────────────────────────────

def upsert_position(ticker: str, strike: float, expiry: str, type_: str,
                     premium_init: float, contracts: int = 1,
                     notes: Optional[str] = None) -> None:
    conn = get_conn()
    now = _dt.datetime.utcnow().isoformat(timespec="seconds")
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO positions (ticker, strike, expiry, type, contracts, premium_init, opened_at, status, notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?) "
            "ON CONFLICT(ticker, strike, expiry, type) DO UPDATE SET "
            "  contracts=excluded.contracts, premium_init=excluded.premium_init, notes=excluded.notes",
            (ticker, strike, expiry, type_, contracts, premium_init, now, notes)
        )
        conn.commit()


def get_open_positions() -> list[sqlite3.Row]:
    conn = get_conn()
    return conn.execute(
        "SELECT * FROM positions WHERE status='OPEN' ORDER BY ticker, expiry"
    ).fetchall()


def close_position(pos_id: int, status: str = "CLOSED") -> None:
    conn = get_conn()
    now = _dt.datetime.utcnow().isoformat(timespec="seconds")
    with _DB_LOCK:
        conn.execute(
            "UPDATE positions SET status=?, closed_at=? WHERE id=?",
            (status, now, pos_id)
        )
        conn.commit()


# ── iv_cache ─────────────────────────────────────────────────────────────────

def get_iv_cache(ticker: str) -> Optional[sqlite3.Row]:
    conn = get_conn()
    return conn.execute("SELECT * FROM iv_cache WHERE ticker=?", (ticker,)).fetchone()


def set_iv_cache(ticker: str, iv_min: float, iv_max: float,
                 iv_current: Optional[float] = None) -> None:
    conn = get_conn()
    now = _dt.datetime.utcnow().isoformat(timespec="seconds")
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO iv_cache (ticker, iv_min_52w, iv_max_52w, iv_current, updated_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(ticker) DO UPDATE SET "
            "  iv_min_52w=excluded.iv_min_52w, iv_max_52w=excluded.iv_max_52w, "
            "  iv_current=COALESCE(excluded.iv_current, iv_cache.iv_current), "
            "  updated_at=excluded.updated_at",
            (ticker, iv_min, iv_max, iv_current, now)
        )
        conn.commit()


def iv_cache_is_stale(ticker: str, max_age_hours: int = 20) -> bool:
    """True if cache missing or older than max_age_hours (default: force daily refresh)."""
    row = get_iv_cache(ticker)
    if row is None:
        return True
    try:
        updated = _dt.datetime.fromisoformat(row["updated_at"])
    except Exception:
        return True
    age_h = (_dt.datetime.utcnow() - updated).total_seconds() / 3600
    return age_h > max_age_hours


# ── macro_events ─────────────────────────────────────────────────────────────

def upsert_macro_event(date: str, event_type: str, impact: str = "HIGH") -> None:
    conn = get_conn()
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO macro_events (date, event_type, impact) VALUES (?, ?, ?) "
            "ON CONFLICT(date, event_type) DO UPDATE SET impact=excluded.impact",
            (date, event_type, impact)
        )
        conn.commit()


def get_macro_events(days_ahead: int = 7, only_high: bool = True) -> list[sqlite3.Row]:
    conn = get_conn()
    today = _dt.date.today().isoformat()
    until = (_dt.date.today() + _dt.timedelta(days=days_ahead)).isoformat()
    q = "SELECT * FROM macro_events WHERE date BETWEEN ? AND ?"
    args: tuple = (today, until)
    if only_high:
        q += " AND impact='HIGH'"
    q += " ORDER BY date ASC, event_type ASC"
    return conn.execute(q, args).fetchall()


def has_macro_event_within(hours: int = 24) -> list[sqlite3.Row]:
    """High-impact events happening in the next `hours`."""
    conn = get_conn()
    now = _dt.datetime.utcnow()
    until = (now + _dt.timedelta(hours=hours)).date().isoformat()
    today = now.date().isoformat()
    return conn.execute(
        "SELECT * FROM macro_events WHERE date BETWEEN ? AND ? AND impact='HIGH'",
        (today, until)
    ).fetchall()


# ── kv_state (generic scratchpad) ────────────────────────────────────────────

def kv_get(key: str) -> Optional[str]:
    conn = get_conn()
    row = conn.execute("SELECT v FROM kv_state WHERE k=?", (key,)).fetchone()
    return row["v"] if row else None


def kv_set(key: str, value: str) -> None:
    conn = get_conn()
    now = _dt.datetime.utcnow().isoformat(timespec="seconds")
    with _DB_LOCK:
        conn.execute(
            "INSERT INTO kv_state (k, v, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at",
            (key, value, now)
        )
        conn.commit()


def db_path() -> str:
    """Exposed for diagnostics / tests."""
    get_conn()
    return _DB_PATH or ""
