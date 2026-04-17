"""
OTU Wheel v2.0 — Position Management (MANAGE alerts)

Scans open option positions from Schwab and fires action triggers:
    TAKE_PROFIT_50    — captured >= 50% of initial premium → close
    ROLL_DECISION     — Monday of expiry week + current delta > 0.40
    ASSIGNMENT_WARNING— price <= strike AND DTE <= 3
    EARNINGS_ALERT    — earnings within 2 days
    FUNDAMENTAL_BREAK — earnings miss detected on last report
    COVERED_CALL_TRIGGER — shares just assigned to us (stock position appeared)

Dedupe: one alert per (ticker, subtype) per 24h via alerts_log.
"""

from __future__ import annotations

import datetime as _dt
import re
from dataclasses import dataclass
from typing import Optional

from . import db, schwab_client, fundamentals

ET_OFFSET_HOURS = -4  # we use UTC in DB, ET for weekday check (good enough)


# ── OSI option symbol parsing ────────────────────────────────────────────────
# Example: "AAPL  260515P00210000" → ticker=AAPL, expiry=2026-05-15, type=P, strike=210.00
_OSI_RE = re.compile(
    r"^(?P<root>[A-Z]{1,6})\s*"
    r"(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})"
    r"(?P<cp>[CP])"
    r"(?P<strike>\d{8})$"
)


def parse_osi(symbol: str) -> Optional[dict]:
    s = symbol.replace(" ", "").strip()
    # re-pad for regex (single spaces normalized above—rebuild with padding)
    m = re.match(
        r"^(?P<root>[A-Z\.]{1,6})(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})(?P<cp>[CP])(?P<strike>\d{8})$",
        s,
    )
    if not m:
        return None
    yy, mm, dd = int(m["yy"]), int(m["mm"]), int(m["dd"])
    year = 2000 + yy
    try:
        expiry = _dt.date(year, mm, dd).isoformat()
    except ValueError:
        return None
    return {
        "ticker": m["root"],
        "expiry": expiry,
        "type":   "PUT" if m["cp"] == "P" else "CALL",
        "strike": int(m["strike"]) / 1000.0,
    }


# ── Trigger definitions ──────────────────────────────────────────────────────

@dataclass
class ManageAlert:
    ticker:   str
    subtype:  str
    severity: str   # INFO / WARN / CRIT
    message:  str
    numbers:  dict


def _is_monday_of_expiry_week(expiry_iso: str) -> bool:
    """True if today is the Monday within the same ISO week as expiry (Fri)."""
    try:
        e = _dt.date.fromisoformat(expiry_iso)
    except Exception:
        return False
    today = _dt.date.today()
    if today.weekday() != 0:   # 0 = Monday
        return False
    e_monday = e - _dt.timedelta(days=e.weekday())
    return e_monday == today


def _days_to_expiry(expiry_iso: str) -> int:
    try:
        e = _dt.date.fromisoformat(expiry_iso)
    except Exception:
        return 999
    return (e - _dt.date.today()).days


# ── Main scanner ─────────────────────────────────────────────────────────────

def scan_open_positions(schwab_headers: dict) -> list[ManageAlert]:
    """
    Returns alerts that have NOT been sent yet in the last 24h.
    Caller is responsible for dispatching to Discord and then calling
    db.log_alert(..., tipo='MANAGE', subtype=...) to record dispatch.
    """
    alerts: list[ManageAlert] = []

    # 1. Pull Schwab positions
    accounts = schwab_client.get_accounts(schwab_headers)
    if not accounts:
        print("  [MANAGE] no Schwab accounts — skipping")
        return alerts

    # Collect stock positions (for COVERED_CALL_TRIGGER) and option positions
    stock_positions: dict[str, int] = {}    # ticker -> shares
    short_options: list[dict] = []          # OSI-parsed + qty/avg

    for acct in accounts:
        acct_hash = acct.get("hashValue") or acct.get("accountNumber")
        if not acct_hash:
            continue
        for p in schwab_client.get_positions(schwab_headers, acct_hash):
            inst = p.get("instrument", {}) or {}
            asset = inst.get("assetType")
            sym   = inst.get("symbol", "")
            long_q  = float(p.get("longQuantity", 0) or 0)
            short_q = float(p.get("shortQuantity", 0) or 0)
            avg_price = float(p.get("averagePrice", 0) or 0)   # per-share for options

            if asset == "EQUITY" and long_q >= 100:
                stock_positions[sym] = stock_positions.get(sym, 0) + int(long_q)

            if asset == "OPTION" and short_q > 0:
                osi = parse_osi(sym)
                if osi:
                    short_options.append({
                        "symbol":      sym,
                        "ticker":      osi["ticker"],
                        "expiry":      osi["expiry"],
                        "type":        osi["type"],
                        "strike":      osi["strike"],
                        "contracts":   int(short_q),
                        "avg_price":   avg_price,           # opening credit per share
                    })

    # 2. Per-option trigger evaluation
    for opt in short_options:
        ticker  = opt["ticker"]
        expiry  = opt["expiry"]
        strike  = opt["strike"]
        type_   = opt["type"]
        contracts = opt["contracts"]

        # Mirror into DB positions table (upsert each scan)
        premium_init = opt["avg_price"] * 100 * contracts
        db.upsert_position(ticker, strike, expiry, type_, premium_init, contracts)

        # Current mark from Schwab
        quote = schwab_client.get_option_quote(schwab_headers, opt["symbol"])
        if quote is None:
            continue
        mark_per_share = quote["mark"]
        premium_now    = mark_per_share * 100 * contracts
        captura_pct    = 0.0 if premium_init <= 0 else (premium_init - premium_now) / premium_init
        current_delta  = abs(quote.get("delta", 0) or 0)

        underlying = schwab_client.get_mark_price(schwab_headers, ticker) or 0.0

        # Trigger A: TAKE_PROFIT_50
        if captura_pct >= 0.50 and not db.was_alerted_recent(ticker, "MANAGE", 24, subtype="TAKE_PROFIT_50"):
            alerts.append(ManageAlert(
                ticker=ticker, subtype="TAKE_PROFIT_50", severity="CRIT",
                message=f"{ticker} {strike:.0f}{type_[0]} {expiry} — captured {captura_pct*100:.0f}% | CLOSE",
                numbers={"captura_pct": round(captura_pct*100, 1),
                         "premium_init": round(premium_init, 2),
                         "premium_now": round(premium_now, 2)},
            ))

        # Trigger B: ROLL_DECISION
        if _is_monday_of_expiry_week(expiry) and current_delta > 0.40 \
           and not db.was_alerted_recent(ticker, "MANAGE", 24, subtype="ROLL_DECISION"):
            alerts.append(ManageAlert(
                ticker=ticker, subtype="ROLL_DECISION", severity="WARN",
                message=f"{ticker} {strike:.0f}{type_[0]} {expiry} — delta {current_delta:.2f}, roll week | REVIEW ROLL",
                numbers={"delta": round(current_delta, 2), "dte": _days_to_expiry(expiry)},
            ))

        # Trigger C: ASSIGNMENT_WARNING
        dte = _days_to_expiry(expiry)
        if type_ == "PUT" and underlying and underlying <= strike and dte <= 3 \
           and not db.was_alerted_recent(ticker, "MANAGE", 24, subtype="ASSIGNMENT_WARNING"):
            alerts.append(ManageAlert(
                ticker=ticker, subtype="ASSIGNMENT_WARNING", severity="CRIT",
                message=f"{ticker} ITM by ${strike - underlying:.2f} | DTE {dte} | ASSIGNMENT RISK",
                numbers={"strike": strike, "underlying": round(underlying, 2), "dte": dte},
            ))

        # Trigger D: EARNINGS_ALERT
        fund = fundamentals.get_fundamentals(ticker)
        e_date = fund.get("earnings_date")
        if e_date:
            try:
                days_to_e = (_dt.date.fromisoformat(e_date) - _dt.date.today()).days
            except Exception:
                days_to_e = 999
            if 0 <= days_to_e <= 2 and not db.was_alerted_recent(ticker, "MANAGE", 24, subtype="EARNINGS_ALERT"):
                alerts.append(ManageAlert(
                    ticker=ticker, subtype="EARNINGS_ALERT", severity="WARN",
                    message=f"{ticker} earnings in {days_to_e}d ({e_date}) | position open",
                    numbers={"days_to_e": days_to_e, "earnings_date": e_date},
                ))

        # Trigger E: FUNDAMENTAL_BREAK — no current earnings beats
        if not fund.get("beats_4q") and type_ == "PUT" \
           and not db.was_alerted_recent(ticker, "MANAGE", 24, subtype="FUNDAMENTAL_BREAK"):
            # Only surface if we're also losing money on the position
            if captura_pct < -0.20:
                alerts.append(ManageAlert(
                    ticker=ticker, subtype="FUNDAMENTAL_BREAK", severity="WARN",
                    message=f"{ticker} not beating earnings + position down {abs(captura_pct)*100:.0f}% | REVIEW",
                    numbers={"captura_pct": round(captura_pct*100, 1)},
                ))

    # 3. Covered-call trigger: stock position appeared that matches a recently assigned put
    for ticker, shares in stock_positions.items():
        # Has a recently-closed put on this ticker existed?
        # Simple heuristic: we have stock AND no open covered call exists — fire once per 24h
        has_open_call = any(
            o["ticker"] == ticker and o["type"] == "CALL" for o in short_options
        )
        if not has_open_call and shares >= 100 \
           and not db.was_alerted_recent(ticker, "MANAGE", 24, subtype="COVERED_CALL_TRIGGER"):
            alerts.append(ManageAlert(
                ticker=ticker, subtype="COVERED_CALL_TRIGGER", severity="INFO",
                message=f"{ticker} — {shares} shares held, no open CALL | SELL COVERED CALL",
                numbers={"shares": shares},
            ))

    return alerts
