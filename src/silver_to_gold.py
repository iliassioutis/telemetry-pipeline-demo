#!/usr/bin/env python3
"""
Silver -> Gold curated outputs.

Reads:
- lake/silver/YYYY-MM-DD/sensor_readings_clean.csv

Writes:
- lake/gold/YYYY-MM-DD/plant_kpis.csv
- lake/gold/YYYY-MM-DD/asset_health_daily.csv
- exports/YYYY-MM-DD/plant_kpis.csv

No external dependencies (stdlib only).
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Create gold KPIs from silver sensor readings."
    )
    p.add_argument(
        "--date", required=True, help="YYYY-MM-DD (must exist under lake/silver/)"
    )
    return p.parse_args()


def to_float(x: Any, default: float = float("nan")) -> float:
    try:
        return float(x)
    except Exception:
        return default


def to_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def safe_mean(values: List[float]) -> float:
    vals = [v for v in values if not math.isnan(v)]
    return sum(vals) / len(vals) if vals else float("nan")


def safe_max(values: List[float]) -> float:
    vals = [v for v in values if not math.isnan(v)]
    return max(vals) if vals else float("nan")


def load_assets_map(bronze_date_dir: Path) -> Dict[str, Dict[str, str]]:
    """
    Load assets.csv from the bronze folder to map asset_id -> plant_id, asset_type, criticality, etc.
    This is a simple join for demo purposes.
    """
    assets_path = bronze_date_dir / "assets.csv"
    if not assets_path.exists():
        return {}

    m: Dict[str, Dict[str, str]] = {}
    with assets_path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            asset_id = (row.get("asset_id") or "").strip()
            if asset_id:
                m[asset_id] = row
    return m


def load_silver(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> int:
    args = parse_args()
    date_str = args.date

    silver_path = Path("lake") / "silver" / date_str / "sensor_readings_clean.csv"
    if not silver_path.exists():
        raise FileNotFoundError(f"Missing input: {silver_path}")

    # For a nicer demo, join with bronze assets to get plant_id + asset_type
    bronze_date_dir = Path("lake") / "bronze" / date_str
    assets_map = load_assets_map(bronze_date_dir)

    rows = load_silver(silver_path)

    # ---- Aggregate per asset ----
    # We'll compute daily summaries used in dashboards / KPI reporting.
    per_asset: Dict[str, Dict[str, Any]] = {}
    per_asset_lists: Dict[str, Dict[str, List[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    running_counts: Dict[str, int] = defaultdict(int)
    total_counts: Dict[str, int] = defaultdict(int)

    for r in rows:
        asset_id = (r.get("asset_id") or "").strip()
        if not asset_id:
            continue

        total_counts[asset_id] += 1
        if (r.get("operating_state") or "").strip() == "running":
            running_counts[asset_id] += 1

        per_asset_lists[asset_id]["temperature_c"].append(
            to_float(r.get("temperature_c"))
        )
        per_asset_lists[asset_id]["vibration_mm_s"].append(
            to_float(r.get("vibration_mm_s"))
        )
        per_asset_lists[asset_id]["pressure_bar"].append(
            to_float(r.get("pressure_bar"))
        )
        per_asset_lists[asset_id]["flow_l_min"].append(to_float(r.get("flow_l_min")))
        per_asset_lists[asset_id]["rpm"].append(float(to_int(r.get("rpm"))))

    asset_health_rows: List[Dict[str, Any]] = []
    for asset_id, metrics in per_asset_lists.items():
        ainfo = assets_map.get(asset_id, {})
        plant_id = (ainfo.get("plant_id") or "UNKNOWN").strip()
        asset_type = (ainfo.get("asset_type") or "UNKNOWN").strip()
        criticality = (ainfo.get("criticality") or "UNKNOWN").strip()
        strategy = (ainfo.get("maintenance_strategy") or "UNKNOWN").strip()

        temp_mean = safe_mean(metrics["temperature_c"])
        vib_mean = safe_mean(metrics["vibration_mm_s"])
        vib_max = safe_max(metrics["vibration_mm_s"])
        rpm_mean = safe_mean(metrics["rpm"])

        run_ratio = (
            (running_counts[asset_id] / total_counts[asset_id])
            if total_counts[asset_id]
            else 0.0
        )

        # Simple "health score" heuristic for demo: higher vibration reduces score
        # Score is not a real model; it shows how you’d publish a business-ready metric.
        health_score = 100.0
        if not math.isnan(vib_mean):
            health_score -= min(40.0, vib_mean * 8.0)
        if not math.isnan(temp_mean):
            # mild penalty for high temps
            if temp_mean > 80:
                health_score -= min(20.0, (temp_mean - 80) * 0.5)

        health_score = max(0.0, min(100.0, health_score))

        asset_health_rows.append(
            {
                "date": date_str,
                "plant_id": plant_id,
                "asset_id": asset_id,
                "asset_type": asset_type,
                "criticality": criticality,
                "maintenance_strategy": strategy,
                "readings": total_counts[asset_id],
                "running_ratio": round(run_ratio, 3),
                "temperature_c_mean": (
                    round(temp_mean, 2) if not math.isnan(temp_mean) else ""
                ),
                "vibration_mm_s_mean": (
                    round(vib_mean, 3) if not math.isnan(vib_mean) else ""
                ),
                "vibration_mm_s_max": (
                    round(vib_max, 3) if not math.isnan(vib_max) else ""
                ),
                "rpm_mean": round(rpm_mean, 0) if not math.isnan(rpm_mean) else "",
                "health_score": round(health_score, 1),
            }
        )

    # ---- Aggregate per plant ----
    plant_lists: Dict[str, Dict[str, List[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    plant_assets: Dict[str, set] = defaultdict(set)
    plant_total_readings: Dict[str, int] = defaultdict(int)

    for row in asset_health_rows:
        plant_id = row["plant_id"]
        plant_assets[plant_id].add(row["asset_id"])
        plant_total_readings[plant_id] += int(row["readings"])
        if row["health_score"] != "":
            plant_lists[plant_id]["health_score"].append(float(row["health_score"]))
        if row["vibration_mm_s_mean"] != "":
            plant_lists[plant_id]["vibration_mm_s_mean"].append(
                float(row["vibration_mm_s_mean"])
            )
        if row["temperature_c_mean"] != "":
            plant_lists[plant_id]["temperature_c_mean"].append(
                float(row["temperature_c_mean"])
            )
        if row["running_ratio"] != "":
            plant_lists[plant_id]["running_ratio"].append(float(row["running_ratio"]))

    plant_kpis_rows: List[Dict[str, Any]] = []
    for plant_id, lists in plant_lists.items():
        plant_kpis_rows.append(
            {
                "date": date_str,
                "plant_id": plant_id,
                "assets_count": len(plant_assets[plant_id]),
                "total_readings": plant_total_readings[plant_id],
                "avg_running_ratio": (
                    round(safe_mean(lists["running_ratio"]), 3)
                    if lists["running_ratio"]
                    else ""
                ),
                "avg_temperature_c": (
                    round(safe_mean(lists["temperature_c_mean"]), 2)
                    if lists["temperature_c_mean"]
                    else ""
                ),
                "avg_vibration_mm_s": (
                    round(safe_mean(lists["vibration_mm_s_mean"]), 3)
                    if lists["vibration_mm_s_mean"]
                    else ""
                ),
                "avg_health_score": (
                    round(safe_mean(lists["health_score"]), 2)
                    if lists["health_score"]
                    else ""
                ),
            }
        )

    # Output paths
    gold_dir = Path("lake") / "gold" / date_str
    exports_dir = Path("exports") / date_str

    write_csv(
        gold_dir / "asset_health_daily.csv",
        fieldnames=[
            "date",
            "plant_id",
            "asset_id",
            "asset_type",
            "criticality",
            "maintenance_strategy",
            "readings",
            "running_ratio",
            "temperature_c_mean",
            "vibration_mm_s_mean",
            "vibration_mm_s_max",
            "rpm_mean",
            "health_score",
        ],
        rows=sorted(asset_health_rows, key=lambda r: (r["plant_id"], r["asset_id"])),
    )

    write_csv(
        gold_dir / "plant_kpis.csv",
        fieldnames=[
            "date",
            "plant_id",
            "assets_count",
            "total_readings",
            "avg_running_ratio",
            "avg_temperature_c",
            "avg_vibration_mm_s",
            "avg_health_score",
        ],
        rows=sorted(plant_kpis_rows, key=lambda r: r["plant_id"]),
    )

    # Copy one output to exports/ for easy viewing
    write_csv(
        exports_dir / "plant_kpis.csv",
        fieldnames=[
            "date",
            "plant_id",
            "assets_count",
            "total_readings",
            "avg_running_ratio",
            "avg_temperature_c",
            "avg_vibration_mm_s",
            "avg_health_score",
        ],
        rows=sorted(plant_kpis_rows, key=lambda r: r["plant_id"]),
    )

    print(f"Read silver: {silver_path}")
    print(f"Gold plant KPIs: {gold_dir / 'plant_kpis.csv'}")
    print(f"Gold asset health: {gold_dir / 'asset_health_daily.csv'}")
    print(f"Exports: {exports_dir / 'plant_kpis.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
