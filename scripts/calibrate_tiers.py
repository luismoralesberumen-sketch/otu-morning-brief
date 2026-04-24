"""
Tier calibration — reads alerts_log + alert_outcomes and reports empirical
win rate and estimated EV per score bucket, per VIX regime, per side.

Usage:
    python -m scripts.calibrate_tiers                # full report
    python -m scripts.calibrate_tiers --min-n 10     # only buckets with n>=10

REQUIRES n >= 10 per bucket before suggesting new thresholds. Below that the
output is diagnostic only — "insufficient data".

Outputs:
  • stdout : per-bucket table + recommended T1/T2 per regime per side
  • ./calibration_report.md : same content as markdown
"""

from __future__ import annotations

import argparse
import math
import sqlite3
from collections import defaultdict
from statistics import mean

from v2 import db


BUCKETS = [(55, 60), (60, 65), (65, 70), (70, 75),
            (75, 80), (80, 85), (85, 90), (90, 101)]

# outcome_class → success boolean for calibration
SUCCESS_CLASSES = {"OTM_SAFE", "EXPIRED_OTM"}
FAILURE_CLASSES = {"BREACHED", "EXPIRED_ITM"}
NEUTRAL_CLASSES = {"ITM_TOUCH"}  # counted as 0.5 (close call)


def wilson_ci(wins: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson 95% CI for a proportion."""
    if n == 0:
        return 0.0, 1.0
    p = wins / n
    denom = 1 + z**2 / n
    center = (p + z**2 / (2*n)) / denom
    margin = (z * math.sqrt(p*(1-p)/n + z**2/(4*n**2))) / denom
    return max(0.0, center - margin), min(1.0, center + margin)


def bucket_for(score: int) -> tuple[int, int]:
    for lo, hi in BUCKETS:
        if lo <= score < hi:
            return (lo, hi)
    return (0, 0)


def fetch_dataset(conn: sqlite3.Connection) -> list[dict]:
    """
    Join alerts with their FINAL outcome (prefer at-expiry=99, else max
    days_since). Returns list of dicts with side, score, tier, roi,
    outcome_class, pnl_est.
    """
    rows = conn.execute("""
        SELECT a.id, a.tipo, a.side, a.score, a.tier, a.strike, a.expiry,
               a.roi_at_alert, a.iv_rank_at_alert, a.timestamp,
               o.days_since, o.outcome_class, o.pnl_est_pct
        FROM alerts_log a
        LEFT JOIN alert_outcomes o ON o.alert_id = a.id
        WHERE a.tipo LIKE 'ENTRY-%'
          AND a.strike IS NOT NULL
          AND a.score IS NOT NULL
        ORDER BY a.id ASC, o.days_since DESC
    """).fetchall()

    # Keep the latest outcome per alert (DESC order → first row per id wins)
    seen: set[int] = set()
    out = []
    for r in rows:
        if r["id"] in seen:
            continue
        seen.add(r["id"])
        out.append(dict(r))
    return out


def summarise(dataset: list[dict], min_n: int) -> str:
    """Produce a markdown report."""
    lines: list[str] = ["# Tier Calibration Report", ""]
    if not dataset:
        lines.append("*No alerts with outcomes found — run outcomes.evaluate_pending first.*")
        return "\n".join(lines)

    lines.append(f"Total alerts with snapshot: **{len(dataset)}**")
    with_outcome = [d for d in dataset if d.get("outcome_class")]
    lines.append(f"With at least one outcome row: **{len(with_outcome)}**")
    lines.append("")

    # Group by side × bucket
    groups: dict[tuple[str, tuple[int, int]], list[dict]] = defaultdict(list)
    for d in with_outcome:
        side_label = d["tipo"].replace("ENTRY-", "")
        bkt = bucket_for(int(d["score"]))
        if bkt == (0, 0):
            continue
        groups[(side_label, bkt)].append(d)

    def _score_row(ds: list[dict]) -> dict:
        n = len(ds)
        wins = sum(1 for x in ds if x["outcome_class"] in SUCCESS_CLASSES)
        losses = sum(1 for x in ds if x["outcome_class"] in FAILURE_CLASSES)
        neutral = sum(1 for x in ds if x["outcome_class"] in NEUTRAL_CLASSES)
        effective = wins + 0.5 * neutral  # neutral counts as half
        wr = effective / n if n else 0.0
        lo, hi = wilson_ci(wins + (neutral // 2 if neutral else 0), n)
        avg_roi = mean([x["roi_at_alert"] for x in ds if x.get("roi_at_alert")]) if ds else 0.0
        avg_pnl = mean([x["pnl_est_pct"] for x in ds if x.get("pnl_est_pct") is not None]) if ds else 0.0
        # EV = WR * avg_roi - (1-WR) * 15%  (15% = max_loss proxy used in Kelly)
        ev = wr * avg_roi - (1 - wr) * 15.0
        return {"n": n, "wins": wins, "losses": losses, "neutral": neutral,
                "wr": wr, "ci_lo": lo, "ci_hi": hi,
                "avg_roi": avg_roi, "avg_pnl": avg_pnl, "ev": ev}

    for side in sorted({s for (s, _) in groups}):
        lines.append(f"## Side: {side}")
        lines.append("")
        lines.append("| Bucket | n | Wins | Loss | Neu | WR | 95% CI | avg ROI | avg P/L% | EV est |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        bucket_stats: list[tuple[tuple[int,int], dict]] = []
        for bkt in BUCKETS:
            ds = groups.get((side, bkt), [])
            if not ds:
                continue
            s = _score_row(ds)
            bucket_stats.append((bkt, s))
            lines.append(
                f"| {bkt[0]}-{bkt[1]} | {s['n']} | {s['wins']} | {s['losses']} | {s['neutral']} | "
                f"{s['wr']*100:.0f}% | [{s['ci_lo']*100:.0f}%, {s['ci_hi']*100:.0f}%] | "
                f"{s['avg_roi']:.2f}% | {s['avg_pnl']:.1f}% | {s['ev']:+.2f} |"
            )
        lines.append("")

        # Recommendation (only if enough data)
        eligible = [(b, s) for b, s in bucket_stats if s["n"] >= min_n]
        if not eligible:
            lines.append(f"_Insufficient data (need n≥{min_n} per bucket) — keep current thresholds._")
            lines.append("")
            continue
        # T1 = lowest bucket lo with WR >= 75%
        # T2 = lowest bucket lo with WR >= 65%
        t1 = next((b[0] for b, s in eligible if s["wr"] >= 0.75), None)
        t2 = next((b[0] for b, s in eligible if s["wr"] >= 0.65), None)
        lines.append(f"**Recommended**: T1 ≥ {t1 or '—'}, T2 ≥ {t2 or '—'} "
                      f"(vs current baseline T1=76, T2=60)")
        # Also EV-optimal bucket
        best_ev = max(eligible, key=lambda x: x[1]["ev"])
        lines.append(f"**EV-optimal bucket**: {best_ev[0][0]}-{best_ev[0][1]} "
                      f"(EV {best_ev[1]['ev']:+.2f}, n={best_ev[1]['n']})")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-n", type=int, default=10)
    args = parser.parse_args()

    conn = db.get_conn()
    dataset = fetch_dataset(conn)
    report = summarise(dataset, args.min_n)

    print(report)
    with open("calibration_report.md", "w", encoding="utf-8") as f:
        f.write(report)
    print("\n[OK] report written to calibration_report.md")


if __name__ == "__main__":
    main()
