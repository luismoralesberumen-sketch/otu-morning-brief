"""
OTU Wheel v2.1 — Alert Actionability Engine

Determines if an alert is immediately actionable or should be queued.

States:
  ENTER_NOW  — todos los criterios OK, ejecutar esta sesión
  QUEUE      — setup válido, esperar condición específica
  WATCH      — setup marginal, monitorear

Checks:
  1. OpEx day (3er viernes del mes) — IV inflado, spreads amplios
  2. Market hours: 10:00 AM - 2:45 PM ET
  3. IVR gate para CALL (comprar): IVR > 65 → spread obligatorio
  4. Bid-ask quality: spread_pct > 12% → fill deficiente
"""

from __future__ import annotations

import datetime as _dt
from typing import Optional

import pytz

ET = pytz.timezone("America/New_York")


def is_opex_day(date: Optional[_dt.date] = None) -> bool:
    """True si es el 3er viernes del mes (vencimiento mensual estándar)."""
    d = date or _dt.date.today()
    if d.weekday() != 4:          # no es viernes
        return False
    friday_count = sum(
        1 for day in range(1, d.day + 1)
        if _dt.date(d.year, d.month, day).weekday() == 4
    )
    return friday_count == 3


def market_hour_ok(now_et: Optional[_dt.datetime] = None) -> tuple[bool, str]:
    """True si está en ventana operativa 10:00-14:45 ET."""
    t = (now_et or _dt.datetime.now(ET)).time()
    if t < _dt.time(10, 0):
        return False, "mercado recién abrió — esperar 10:00 AM ET"
    if t >= _dt.time(14, 45):
        return False, "cerca del cierre — esperar próxima sesión"
    return True, ""


def _recommend_structure(side: str, ivr: float, tier: Optional[int]) -> str:
    if side == "PUT":
        return "Cash Secured Put (CSP)"
    if ivr > 65:
        return "Bull Call Spread — IVR elevado, spread neutraliza costo de IV"
    if ivr > 45:
        return "Bull Call Spread recomendado"
    if tier == 1:
        return "LEAP Deep ITM ~0.79Δ (2027+)"
    return "Bull Call Spread / LEAP ATM"


def next_entry_window(now_et: Optional[_dt.datetime] = None) -> str:
    """Próxima ventana de entrada en texto."""
    now   = now_et or _dt.datetime.now(ET)
    today = now.date()

    if is_opex_day(today):
        nxt = today + _dt.timedelta(days=3)   # viernes → lunes
        return f"Lunes {nxt.strftime('%d %b')} — 10:00 AM ET"

    t = now.time()
    if t < _dt.time(10, 0):
        return f"Hoy {today.strftime('%d %b')} — 10:00 AM ET"
    if t >= _dt.time(14, 45):
        days = 3 if today.weekday() == 4 else 1
        nxt  = today + _dt.timedelta(days=days)
        label = "Lunes" if days == 3 else "Mañana"
        return f"{label} {nxt.strftime('%d %b')} — 10:00 AM ET"

    return "Ahora — ventana abierta"


def get_action_status(
    side:        str,
    iv_rank:     Optional[float],
    spread_pct:  Optional[float],
    score:       int,
    tier:        Optional[int],
    now_et:      Optional[_dt.datetime] = None,
) -> tuple[str, list[str], str]:
    """
    Returns (status, reasons, structure_rec).
      status        : "ENTER_NOW" | "QUEUE" | "WATCH"
      reasons       : razones en español por las que no es ENTER_NOW
      structure_rec : estructura de trade recomendada
    """
    now   = now_et or _dt.datetime.now(ET)
    today = now.date()
    ivr   = iv_rank or 0
    reasons: list[str] = []

    # 1. OpEx
    if is_opex_day(today):
        reasons.append("OpEx day — IV inflado y spreads amplios")

    # 2. Market hours
    hr_ok, hr_msg = market_hour_ok(now)
    if not hr_ok:
        reasons.append(hr_msg)

    # 3. IVR para CALL (comprar calls es caro cuando IVR alto)
    if side == "CALL" and ivr > 65:
        reasons.append(f"IVR {ivr:.0f} — premium caro para comprar calls")

    # 4. Bid-ask spread
    if spread_pct is not None and spread_pct > 12.0:
        reasons.append(f"spread bid-ask {spread_pct:.1f}% — fill deficiente")

    structure_rec = _recommend_structure(side, ivr, tier)

    if not reasons:
        status = "ENTER_NOW"
    elif tier is not None:
        status = "QUEUE"
    else:
        status = "WATCH"

    return status, reasons, structure_rec
