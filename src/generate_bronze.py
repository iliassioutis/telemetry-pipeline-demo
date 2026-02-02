#!/usr/bin/env python3
"""
Generate synthetic industrial operations data into lake/bronze/YYYY-MM-DD/.

Outputs (Bronze - raw landed):
- plants.csv
- assets.csv
- sensor_readings.jsonl
- work_orders.csv
- quality_inspections.csv
- generation_meta.json

No external dependencies (stdlib only).
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# ----------------------------
# Helpers
# ----------------------------

def iso_utc(dt: datetime) -> str:
    """Return ISO 8601 UTC timestamp with Z suffix."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rand_choice(rng: random.Random, items: List[str], weights: Optional[List[float]] = None) -> str:
    if weights:
        return rng.choices(items, weights=weights, k=1)[0]
    return rng.choice(items)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def maybe_null(rng: random.Random, value: Any, p_null: float) -> Any:
    return None if rng.random() < p_null else value


# ----------------------------
# Data models
# ----------------------------

@dataclass
class Plant:
    plant_id: str
    plant_name: str
    country: str
    timezone: str
    line_count: int


@dataclass
class Asset:
    asset_id: str
    plant_id: str
    asset_type: str
    manufacturer: str
    model: str
    install_date: str
    criticality: str
    maintenance_strategy: str


# ----------------------------
# Generation
# ----------------------------

def generate_plants(rng: random.Random, n: int) -> List[Plant]:
    candidates = [
        ("Athens", "GR", "Europe/Athens"),
        ("Berlin", "DE", "Europe/Berlin"),
        ("Stamford", "US", "America/New_York"),
        ("Krakow", "PL", "Europe/Warsaw"),
        ("Bangalore", "IN", "Asia/Kolkata"),
        ("Monterrey", "MX", "America/Monterrey"),
    ]
    rng.shuffle(candidates)
    chosen = candidates[:n]

    plants: List[Plant] = []
    for i, (city, country, tz) in enumerate(chosen, start=1):
        plants.append(
            Plant(
                plant_id=f"PLT-{i:03d}",
                plant_name=f"{city} Plant",
                country=country,
                timezone=tz,
                line_count=rng.randint(2, 8),
            )
        )
    return plants


def generate_assets(rng: random.Random, plants: List[Plant], assets_per_plant: int) -> List[Asset]:
    asset_types = ["pump", "motor", "compressor", "valve", "conveyor", "fan"]
    manufacturers = ["Goulds", "KONI", "Cannon", "Enidine", "Bornemann", "Rheinhütte"]
    criticalities = ["low", "med", "high"]
    strategies = ["preventive", "predictive", "run-to-failure"]

    assets: List[Asset] = []
    a = 1
    for p in plants:
        for _ in range(assets_per_plant):
            asset_type = rand_choice(rng, asset_types)
            manufacturer = rand_choice(rng, manufacturers)
            model = f"{asset_type.upper()}-{rng.randint(100,999)}"
            install_dt = date.today() - timedelta(days=rng.randint(30, 3650))
            criticality = rand_choice(rng, criticalities, weights=[0.35, 0.45, 0.20])
            # Strategy tends to depend on criticality
            if criticality == "high":
                strategy = rand_choice(rng, ["preventive", "predictive"], weights=[0.4, 0.6])
            elif criticality == "med":
                strategy = rand_choice(rng, strategies, weights=[0.45, 0.35, 0.20])
            else:
                strategy = rand_choice(rng, strategies, weights=[0.30, 0.15, 0.55])

            assets.append(
                Asset(
                    asset_id=f"AST-{a:05d}",
                    plant_id=p.plant_id,
                    asset_type=asset_type,
                    manufacturer=manufacturer,
                    model=model,
                    install_date=str(install_dt),
                    criticality=criticality,
                    maintenance_strategy=strategy,
                )
            )
            a += 1
    return assets


def base_signals_for(asset_type: str) -> Dict[str, float]:
    """
    Provide typical baselines by asset type.
    Values are intentionally approximate to look realistic.
    """
    if asset_type == "pump":
        return {"temperature_c": 55, "vibration_mm_s": 2.0, "pressure_bar": 6.5, "flow_l_min": 120, "rpm": 2900}
    if asset_type == "compressor":
        return {"temperature_c": 65, "vibration_mm_s": 2.5, "pressure_bar": 8.0, "flow_l_min": 80, "rpm": 3600}
    if asset_type == "motor":
        return {"temperature_c": 50, "vibration_mm_s": 1.6, "pressure_bar": 0.0, "flow_l_min": 0.0, "rpm": 3000}
    if asset_type == "fan":
        return {"temperature_c": 45, "vibration_mm_s": 1.2, "pressure_bar": 0.0, "flow_l_min": 0.0, "rpm": 1800}
    if asset_type == "valve":
        return {"temperature_c": 40, "vibration_mm_s": 0.7, "pressure_bar": 5.0, "flow_l_min": 90, "rpm": 0.0}
    # conveyor
    return {"temperature_c": 42, "vibration_mm_s": 1.0, "pressure_bar": 0.0, "flow_l_min": 0.0, "rpm": 120}


def generate_sensor_readings(
    rng: random.Random,
    assets: List[Asset],
    day: date,
    sample_minutes: int,
    bad_rate: float,
) -> (List[Dict[str, Any]], Dict[str, int]):
    """
    Generate JSONL-friendly dict rows.
    We'll inject a small fraction of "bad" rows: missing asset_id, impossible values, duplicate IDs.
    """
    start = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc)
    steps = int((24 * 60) / sample_minutes)

    rows: List[Dict[str, Any]] = []
    counters = {"total": 0, "bad_missing_asset_id": 0, "bad_out_of_range": 0, "bad_duplicate_id": 0}

    reading_seq = 1
    duplicate_id: Optional[str] = None

    for asset in assets:
        base = base_signals_for(asset.asset_type)
        # Simple degradation factor: some assets run "rougher"
        roughness = rng.uniform(0.85, 1.25)

        for s in range(steps):
            ts = start + timedelta(minutes=sample_minutes * s)

            # operating state distribution
            state = rand_choice(rng, ["running", "idle", "off"], weights=[0.78, 0.15, 0.07])
            if state == "off":
                # mostly zeros when off
                temp = base["temperature_c"] * 0.7 + rng.uniform(-1, 1)
                vib = rng.uniform(0.0, 0.3)
                rpm = 0.0
            elif state == "idle":
                temp = base["temperature_c"] * 0.9 + rng.uniform(-2, 2)
                vib = base["vibration_mm_s"] * 0.6 + rng.uniform(-0.3, 0.3)
                rpm = base["rpm"] * 0.35 + rng.uniform(-50, 50)
            else:
                temp = base["temperature_c"] * roughness + rng.uniform(-3, 3)
                vib = base["vibration_mm_s"] * roughness + rng.uniform(-0.6, 0.6)
                rpm = base["rpm"] * clamp(roughness, 0.9, 1.15) + rng.uniform(-80, 80)

            pressure = base["pressure_bar"] * (1.0 if state == "running" else 0.2) + rng.uniform(-0.3, 0.3)
            flow = base["flow_l_min"] * (1.0 if state == "running" else 0.1) + rng.uniform(-5, 5)

            # Build row
            reading_id = f"RDG-{reading_seq:08d}"
            reading_seq += 1

            row: Dict[str, Any] = {
                "reading_id": reading_id,
                "asset_id": asset.asset_id,
                "ts_utc": iso_utc(ts),
                "temperature_c": round(temp, 2),
                "vibration_mm_s": round(max(0.0, vib), 3),
                "pressure_bar": round(max(0.0, pressure), 3),
                "flow_l_min": round(max(0.0, flow), 2),
                "rpm": int(max(0.0, rpm)),
                "operating_state": state,
                "sample_interval_sec": sample_minutes * 60,
            }

            # Inject controlled "bad" data
            if rng.random() < bad_rate:
                bad_type = rand_choice(rng, ["missing_asset_id", "out_of_range", "duplicate_id"], weights=[0.35, 0.45, 0.20])
                if bad_type == "missing_asset_id":
                    row["asset_id"] = ""  # missing required ID
                    counters["bad_missing_asset_id"] += 1
                elif bad_type == "out_of_range":
                    # make an impossible value (negative pressure OR extreme temp)
                    if rng.random() < 0.5:
                        row["pressure_bar"] = -3.0
                    else:
                        row["temperature_c"] = 250.0
                    counters["bad_out_of_range"] += 1
                else:
                    # duplicate reading_id
                    if duplicate_id is None:
                        duplicate_id = row["reading_id"]
                    else:
                        row["reading_id"] = duplicate_id
                    counters["bad_duplicate_id"] += 1

            rows.append(row)
            counters["total"] += 1

    return rows, counters


def generate_work_orders(rng: random.Random, assets: List[Asset], day: date) -> List[Dict[str, Any]]:
    teams = ["Mechanical", "Electrical", "Reliability", "Ops"]
    failure_modes = ["BEARING", "SEAL_LEAK", "OVERHEAT", "MISALIGNMENT", "CAVITATION"]
    root_causes = ["LUBRICATION", "INSTALLATION", "WEAR", "CONTAMINATION", "OPERATOR_ERROR"]

    rows: List[Dict[str, Any]] = []
    seq = 1

    # A small number of work orders per day across all assets
    wo_count = max(3, int(len(assets) * 0.15))
    chosen_assets = rng.sample(assets, k=min(wo_count, len(assets)))

    for asset in chosen_assets:
        created = datetime(day.year, day.month, day.day, rng.randint(0, 23), rng.choice([0, 15, 30, 45]), tzinfo=timezone.utc)
        duration_min = rng.randint(15, 8 * 60)
        closed = created + timedelta(minutes=duration_min)

        wo_type = rand_choice(rng, ["corrective", "preventive"], weights=[0.55, 0.45])
        priority = rand_choice(rng, ["P1", "P2", "P3", "P4"], weights=[0.10, 0.25, 0.45, 0.20])

        # Some work orders won't have failure/root cause (preventive or not yet diagnosed)
        failure_mode = maybe_null(rng, rand_choice(rng, failure_modes), p_null=0.35 if wo_type == "preventive" else 0.15)
        root_cause = maybe_null(rng, rand_choice(rng, root_causes), p_null=0.55)

        row = {
            "wo_id": f"WO-{seq:07d}",
            "asset_id": asset.asset_id,
            "created_ts_utc": iso_utc(created),
            "closed_ts_utc": iso_utc(closed),
            "wo_type": wo_type,
            "priority": priority,
            "status": "closed",
            "technician_team": rand_choice(rng, teams),
            "downtime_minutes": duration_min if wo_type == "corrective" else int(duration_min * 0.3),
            "parts_cost_eur": round(max(0.0, rng.gauss(180.0, 120.0)), 2),
            "failure_mode_code": failure_mode,
            "root_cause_code": root_cause,
        }
        rows.append(row)
        seq += 1

    return rows


def generate_quality_inspections(rng: random.Random, plants: List[Plant], day: date) -> List[Dict[str, Any]]:
    product_families = ["Industrial-Pump", "Valve-Assembly", "Connector-Series", "Shock-Absorber"]
    defect_codes = ["DIMENSION_OUT", "SURFACE_DEFECT", "LEAK_TEST_FAIL", "ELECTRICAL_FAIL", "LABEL_MISMATCH"]

    rows: List[Dict[str, Any]] = []
    seq = 1

    for p in plants:
        # a few inspections per line
        for line_id in range(1, p.line_count + 1):
            n = rng.randint(1, 3)
            for _ in range(n):
                ts = datetime(day.year, day.month, day.day, rng.randint(0, 23), rng.choice([0, 15, 30, 45]), tzinfo=timezone.utc)
                family = rand_choice(rng, product_families)
                batch_id = f"BAT-{p.plant_id}-{day.strftime('%Y%m%d')}-{rng.randint(100,999)}"

                fail = rng.random() < 0.08  # ~8% fail rate
                result = "fail" if fail else "pass"
                defect_code = rand_choice(rng, defect_codes) if fail else None
                severity = rand_choice(rng, ["low", "med", "high"], weights=[0.55, 0.35, 0.10]) if fail else None

                rows.append(
                    {
                        "inspection_id": f"INSP-{seq:07d}",
                        "plant_id": p.plant_id,
                        "line_id": f"L{line_id:02d}",
                        "ts_utc": iso_utc(ts),
                        "product_family": family,
                        "batch_id": batch_id,
                        "result": result,
                        "defect_code": defect_code,
                        "defect_severity": severity,
                    }
                )
                seq += 1

    return rows


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic industrial bronze data.")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: today)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--plants", type=int, default=3, help="Number of plants (default: 3)")
    parser.add_argument("--assets-per-plant", type=int, default=10, help="Assets per plant (default: 10)")
    parser.add_argument("--sample-minutes", type=int, default=15, help="Sensor sampling interval in minutes (default: 15)")
    parser.add_argument("--bad-rate", type=float, default=0.015, help="Fraction of bad sensor rows (default: 0.015)")
    args = parser.parse_args()

    day = date.today() if not args.date else datetime.strptime(args.date, "%Y-%m-%d").date()
    rng = random.Random(args.seed)

    out_dir = Path("lake") / "bronze" / day.strftime("%Y-%m-%d")
    out_dir.mkdir(parents=True, exist_ok=True)

    plants = generate_plants(rng, n=args.plants)
    assets = generate_assets(rng, plants=plants, assets_per_plant=args.assets_per_plant)

    sensor_rows, sensor_counters = generate_sensor_readings(
        rng, assets=assets, day=day, sample_minutes=args.sample_minutes, bad_rate=args.bad_rate
    )
    wo_rows = generate_work_orders(rng, assets=assets, day=day)
    insp_rows = generate_quality_inspections(rng, plants=plants, day=day)

    # Write plants/assets
    write_csv(
        out_dir / "plants.csv",
        [p.__dict__ for p in plants],
        fieldnames=["plant_id", "plant_name", "country", "timezone", "line_count"],
    )
    write_csv(
        out_dir / "assets.csv",
        [a.__dict__ for a in assets],
        fieldnames=["asset_id", "plant_id", "asset_type", "manufacturer", "model", "install_date", "criticality", "maintenance_strategy"],
    )

    # Write work orders / inspections
    write_csv(
        out_dir / "work_orders.csv",
        wo_rows,
        fieldnames=[
            "wo_id", "asset_id", "created_ts_utc", "closed_ts_utc", "wo_type", "priority", "status",
            "technician_team", "downtime_minutes", "parts_cost_eur", "failure_mode_code", "root_cause_code"
        ],
    )
    write_csv(
        out_dir / "quality_inspections.csv",
        insp_rows,
        fieldnames=[
            "inspection_id", "plant_id", "line_id", "ts_utc", "product_family", "batch_id", "result", "defect_code", "defect_severity"
        ],
    )

    # Sensor readings as JSONL (keeps nulls, flexible schema)
    write_jsonl(out_dir / "sensor_readings.jsonl", sensor_rows)

    meta = {
        "generated_for_date": day.strftime("%Y-%m-%d"),
        "seed": args.seed,
        "plants": args.plants,
        "assets_per_plant": args.assets_per_plant,
        "sample_minutes": args.sample_minutes,
        "bad_rate": args.bad_rate,
        "counts": {
            "plants": len(plants),
            "assets": len(assets),
            "sensor_rows": len(sensor_rows),
            "work_orders": len(wo_rows),
            "quality_inspections": len(insp_rows),
        },
        "sensor_bad_counters": sensor_counters,
        "output_dir": str(out_dir).replace("\\", "/"),
    }
    (out_dir / "generation_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"Wrote bronze data to: {out_dir}")
    print(json.dumps(meta, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
