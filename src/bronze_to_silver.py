#!/usr/bin/env python3
"""
Bronze -> Silver transform for sensor_readings.

Reads:
- lake/bronze/YYYY-MM-DD/sensor_readings.jsonl

Writes:
- lake/silver/YYYY-MM-DD/sensor_readings_clean.csv
- lake/quarantine/YYYY-MM-DD/sensor_readings_rejects.csv
- reports/dq_YYYY-MM-DD.md

No external dependencies (stdlib only).
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Transform bronze sensor readings to silver with validation + quarantine.")
    p.add_argument("--date", required=True, help="YYYY-MM-DD (must exist under lake/bronze/)")
    return p.parse_args()


def is_iso_utc_z(ts: str) -> bool:
    # simple check for ISO UTC like 2026-02-02T12:30:00Z
    return isinstance(ts, str) and ts.endswith("Z") and "T" in ts


def validate_row(r: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate a sensor reading row. Return (is_valid, reasons[]).
    """
    reasons: List[str] = []

    reading_id = (r.get("reading_id") or "").strip()
    asset_id = (r.get("asset_id") or "").strip()
    ts_utc = r.get("ts_utc")

    # Required fields
    if not reading_id:
        reasons.append("missing_reading_id")
    if not asset_id:
        reasons.append("missing_asset_id")
    if not ts_utc or not is_iso_utc_z(ts_utc):
        reasons.append("bad_ts_utc")

    # Numeric range checks (very basic but realistic)
    # These are not "physics perfect"; they are sanity checks.
    def as_float(x: Any) -> float:
        try:
            return float(x)
        except Exception:
            return float("nan")

    temperature = as_float(r.get("temperature_c"))
    vibration = as_float(r.get("vibration_mm_s"))
    pressure = as_float(r.get("pressure_bar"))
    flow = as_float(r.get("flow_l_min"))
    rpm = as_float(r.get("rpm"))

    # Temperature sanity: -40 to 200 C
    if not (-40.0 <= temperature <= 200.0):
        reasons.append("temperature_out_of_range")
    # Vibration sanity: 0 to 50 mm/s
    if not (0.0 <= vibration <= 50.0):
        reasons.append("vibration_out_of_range")
    # Pressure sanity: 0 to 50 bar
    if not (0.0 <= pressure <= 50.0):
        reasons.append("pressure_out_of_range")
    # Flow sanity: 0 to 5000 L/min
    if not (0.0 <= flow <= 5000.0):
        reasons.append("flow_out_of_range")
    # RPM sanity: 0 to 20000
    if not (0.0 <= rpm <= 20000.0):
        reasons.append("rpm_out_of_range")

    return (len(reasons) == 0, reasons)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_dq_report(path: Path, date_str: str, stats: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    lines.append(f"# Data Quality report — {date_str}\n")
    lines.append("This report summarizes validation results for `sensor_readings` during Bronze → Silver.\n")
    lines.append("## Summary\n")
    lines.append(f"- Total rows read: **{stats['total']}**\n")
    lines.append(f"- Clean rows written: **{stats['clean']}**\n")
    lines.append(f"- Rejected rows written (quarantine): **{stats['rejects']}**\n")
    lines.append(f"- Duplicate reading_id rejected: **{stats['dup_rejects']}**\n")
    lines.append("\n## Reject reasons (top)\n")
    for reason, count in stats["reasons"].items():
        lines.append(f"- {reason}: {count}\n")
    path.write_text("".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    date_str = args.date

    bronze_dir = Path("lake") / "bronze" / date_str
    in_path = bronze_dir / "sensor_readings.jsonl"
    if not in_path.exists():
        raise FileNotFoundError(f"Missing input: {in_path}")

    rows = load_jsonl(in_path)

    seen_ids = set()
    clean_rows: List[Dict[str, Any]] = []
    reject_rows: List[Dict[str, Any]] = []

    reason_counts: Dict[str, int] = {}
    dup_rejects = 0

    for r in rows:
        reading_id = (r.get("reading_id") or "").strip()

        # De-duplication by reading_id
        if reading_id and reading_id in seen_ids:
            dup_rejects += 1
            rr = dict(r)
            rr["reject_reason"] = "duplicate_reading_id"
            reject_rows.append(rr)
            reason_counts["duplicate_reading_id"] = reason_counts.get("duplicate_reading_id", 0) + 1
            continue

        if reading_id:
            seen_ids.add(reading_id)

        valid, reasons = validate_row(r)
        if valid:
            clean_rows.append(r)
        else:
            rr = dict(r)
            rr["reject_reason"] = "|".join(reasons)
            reject_rows.append(rr)
            for reason in reasons:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

    # Output paths
    silver_dir = Path("lake") / "silver" / date_str
    quarantine_dir = Path("lake") / "quarantine" / date_str
    reports_dir = Path("reports")

    clean_fields = [
        "reading_id", "asset_id", "ts_utc",
        "temperature_c", "vibration_mm_s", "pressure_bar", "flow_l_min", "rpm",
        "operating_state", "sample_interval_sec"
    ]
    reject_fields = clean_fields + ["reject_reason"]

    write_csv(silver_dir / "sensor_readings_clean.csv", clean_fields, clean_rows)
    write_csv(quarantine_dir / "sensor_readings_rejects.csv", reject_fields, reject_rows)

    stats = {
        "total": len(rows),
        "clean": len(clean_rows),
        "rejects": len(reject_rows),
        "dup_rejects": dup_rejects,
        "reasons": dict(sorted(reason_counts.items(), key=lambda kv: kv[1], reverse=True)),
    }

    write_dq_report(reports_dir / f"dq_{date_str}.md", date_str, stats)

    print(f"Read: {in_path}")
    print(f"Silver: {silver_dir / 'sensor_readings_clean.csv'}  ({len(clean_rows)} rows)")
    print(f"Quarantine: {quarantine_dir / 'sensor_readings_rejects.csv'}  ({len(reject_rows)} rows)")
    print(f"DQ report: {reports_dir / f'dq_{date_str}.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
