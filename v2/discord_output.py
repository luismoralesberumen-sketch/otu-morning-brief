"""
OTU Wheel v2.0 — Discord formatting + dispatch

Three output formats:
  • morning_brief_message()  — ENTRY-CSP tabla Kelly-ranked
  • leap_alert_message()     — ENTRY-LEAP individual por ticker
  • manage_message()         — MANAGE inline critical
"""

from __future__ import annotations

import datetime as _dt
import time
from typing import Iterable, Optional

import pytz
import requests


ET = pytz.timezone("America/New_York")


# ── Low-level send ───────────────────────────────────────────────────────────

def send(webhook_url: str, content: str) -> bool:
    """Split at Discord's 2000-char limit and send. Returns True if all chunks 2xx."""
    if not webhook_url:
        return False
    chunks = [content[i:i + 1990] for i in range(0, len(content), 1990)] or [""]
    all_ok = True
    for chunk in chunks:
        try:
            r = requests.post(webhook_url, json={"content": chunk}, timeout=10)
            if r.status_code not in (200, 204):
                print(f"  [DISCORD] {r.status_code} {r.text[:120]}")
                all_ok = False
        except Exception as e:
            print(f"  [DISCORD] send error: {e}")
            all_ok = False
        time.sleep(0.5)
    return all_ok


# ── Morning Brief (ENTRY-CSP) ────────────────────────────────────────────────

def morning_brief_message(
    slot_label: str,
    macro: dict,
    qualified: list[dict],
    upcoming_events: list[dict],
    total_scanned: int,
    target_expiry: str,
    vix_rule: str,
) -> str:
    """
    qualified rows must include:
      ticker, price, strike, delta, mid, iv_rank, roi, kelly, dte, flags
    Sorted by kelly desc.
    """
    now_et = _dt.datetime.now(ET)
    bear   = "BEAR MARKET" if macro.get("bear_market") else "No Bear"

    lines: list[str] = []
    lines.append(f"# OTU Morning Brief — {now_et.strftime('%a %b %d, %Y')} | {slot_label} ET")
    lines.append("")
    lines.append(
        f"**VIX:** {macro.get('vix','?')} | "
        f"**SPY:** ${macro.get('spy','?')} vs EMA200 ${macro.get('ema200','?')} | {bear}"
    )
    lines.append(f"**OTU Rule:** {vix_rule}")

    if upcoming_events:
        ev = " | ".join(f"{e['event_type']} {e['date'][5:]}" for e in upcoming_events[:5])
        lines.append(f"**Macro next 5d:** {ev}")
    else:
        lines.append("**Macro next 5d:** (no high-impact events)")

    lines.append("")
    lines.append(
        f"**CSP Scan | Target exp {target_expiry} | ~30Δ PUT | Kelly-ranked**"
    )
    lines.append(f"Found **{len(qualified)}/{total_scanned}** qualifying trades")
    lines.append("")

    if qualified:
        lines.append("```")
        hdr = f"{'#':<2} {'Tkr':<5} {'Px':>7} {'Str':>6} {'Δ':>5} {'Mid':>5} {'IVR':>4} {'ROI':>5} {'Kelly':>6} {'DTE':>3} Flags"
        lines.append(hdr)
        lines.append("-" * min(len(hdr), 72))
        for i, r in enumerate(qualified[:15], 1):
            flags = ",".join(r.get("flags", [])) or "-"
            lines.append(
                f"{i:<2} {r['ticker']:<5} ${r['price']:>6.2f} "
                f"{r['strike']:>5.0f} {r['delta']:>5.2f} "
                f"${r['mid']:>4.2f} {r.get('iv_rank','?'):>3.0f} "
                f"{r['roi']:>4.2f}% {r.get('kelly',0):>5.1f} "
                f"{r.get('dte','?'):>3} {flags[:20]}"
            )
        lines.append("```")

        lines.append("")
        lines.append("**Top 5 picks (by Kelly)**")
        for i, r in enumerate(qualified[:5], 1):
            reason = _one_line_reason(r)
            lines.append(
                f"{i}. **{r['ticker']}** ${r['strike']:.0f}P @ ${r['mid']:.2f} | "
                f"ROI {r['roi']:.2f}% | IVR {r.get('iv_rank',0):.0f} | "
                f"Kelly {r.get('kelly',0):.1f} — *{reason}*"
            )
    else:
        lines.append("*No qualifying trades — all candidates failed hard filters or Kelly <= 0.*")

    lines.append("")
    lines.append(f"*Data: Schwab API + Yahoo VIX | {now_et.strftime('%H:%M ET')}*")
    return "\n".join(lines)


def _one_line_reason(r: dict) -> str:
    ivr = r.get("iv_rank", 0)
    wr  = r.get("backtest_wr", 50)
    parts = []
    if ivr >= 50: parts.append(f"IVR {ivr:.0f} rich vol")
    if wr  >= 65: parts.append(f"backtest WR {wr}%")
    if r.get("pe_positive") and r.get("beats_4q"): parts.append("fundamentals clean")
    return ", ".join(parts) or "composite edge"


# ── LEAP Alert (ENTRY-LEAP) ──────────────────────────────────────────────────

def leap_alert_message(ticker: str, score: int, tier: int, tier_desc: str,
                        d: dict, iv_rank: Optional[float], kelly: Optional[float],
                        prev_tier: Optional[int] = None) -> str:
    now_et = _dt.datetime.now(ET)
    upgrade = f" | T{prev_tier} → T{tier} UPGRADE" if prev_tier and tier < prev_tier else ""
    emoji   = {1: "🚀", 2: "📊"}.get(tier, "🔔")

    price   = d.get("price", 0)
    rsi     = d.get("rsi", "—")
    low_bb  = d.get("lower_bb")
    bb_str  = f"${low_bb:.2f}" if low_bb else "—"
    ema200  = d.get("ema200")
    ema_str = f"${ema200:.2f}" if ema200 else "—"
    wr      = d.get("backtest_wr", "—")

    ivr_line   = f"IV Rank:     {iv_rank:.0f}"   if iv_rank is not None else "IV Rank:     —"
    kelly_line = f"Kelly Score: {kelly:.1f}"     if kelly   is not None else "Kelly Score: —"

    msg = (
        f"## {emoji} LEAP ALERT — **{ticker}**{upgrade}\n"
        f"```\n"
        f"Price:       ${price:.2f}\n"
        f"Score:       {score}/100   Tier {tier}: {tier_desc}\n"
        f"{'-'*46}\n"
        f"RSI(14):     {rsi}\n"
        f"Lower BB:    {bb_str}\n"
        f"EMA200:      {ema_str}\n"
        f"{ivr_line}\n"
        f"{kelly_line}\n"
        f"Backtest WR: {wr}%\n"
        f"{'-'*46}\n"
        f"Strategy:    {tier_desc}\n"
        f"```\n"
        f"*{now_et.strftime('%I:%M %p ET')} — OTU Wheel v2.0*"
    )
    return msg


# ── MANAGE ───────────────────────────────────────────────────────────────────

_SEV_EMOJI = {"INFO": "🔵", "WARN": "🟡", "CRIT": "🔴"}

def manage_message(alert) -> str:
    """alert: ManageAlert from manage_module."""
    emoji = _SEV_EMOJI.get(alert.severity, "🔔")
    return f"{emoji} **MANAGE — {alert.subtype}** | {alert.message}"


def manage_batch_message(alerts: list) -> str:
    if not alerts:
        return ""
    now_et = _dt.datetime.now(ET)
    lines = [f"## 🧭 MANAGE Scan — {now_et.strftime('%I:%M %p ET')}"]
    for a in alerts:
        lines.append(manage_message(a))
    return "\n".join(lines)


# ── Summary footer (always sent at end of scan) ──────────────────────────────

def scan_summary_message(tipo: str, scanned: int, sent: int,
                         extras: Optional[str] = None) -> str:
    now_et = _dt.datetime.now(ET)
    base = (
        f"*🔍 {tipo} scan — {sent} sent / {scanned} scanned "
        f"| {now_et.strftime('%I:%M %p ET')}*"
    )
    return base + (f" — {extras}" if extras else "")
