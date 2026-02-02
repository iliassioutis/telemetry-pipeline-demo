# Scenario & data model (synthetic)

This demo simulates a multi-plant industrial data consolidation pipeline.

## Plants
Multiple manufacturing sites (plants) generate operational data daily.

**Key fields**
- plant_id, plant_name, country, timezone, line_count

## Assets (equipment master)
Represents equipment installed at each plant (e.g., pumps, motors, compressors, valves, conveyors).

**Key fields**
- asset_id, plant_id, asset_type, manufacturer, model, install_date
- criticality (low/med/high), maintenance_strategy (preventive/predictive/run-to-failure)

## Sensor readings (time series)
High-volume operational telemetry for each asset.

**Key fields**
- reading_id, asset_id, ts_utc
- temperature_c, vibration_mm_s, pressure_bar, flow_l_min, rpm
- operating_state (idle/running/off), sample_interval_sec

## Maintenance work orders
Corrective and preventive maintenance events.

**Key fields**
- wo_id, asset_id, created_ts_utc, closed_ts_utc
- wo_type (corrective/preventive), priority (P1-P4), status
- technician_team, downtime_minutes, parts_cost_eur
- failure_mode_code (nullable), root_cause_code (nullable)

## Quality inspections
Production quality checks, tied to a plant line and a time window.

**Key fields**
- inspection_id, plant_id, line_id, ts_utc
- product_family, batch_id, result (pass/fail)
- defect_code (nullable), defect_severity (low/med/high)

## Data quality rules (examples)
- Required IDs present (plant_id, asset_id, ts_utc, etc.)
- Timestamps parseable and standardized to UTC
- Numerical ranges enforced (e.g., pressure >= 0, temperature within plausible limits)
- Deduplication by stable keys (e.g., reading_id / wo_id)
- Quarantine invalid rows with reason codes
