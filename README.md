# Industrial operations & maintenance data pipeline (synthetic)

This repo demonstrates an "Azure-style" data pipeline using free tooling, designed to mirror a typical
manufacturing / industrial analytics scenario (multi-plant data consolidation).

## Architecture

![Telemetry pipeline architecture](docs/diagrams/architecture.png)

## Scenario
Daily ingest of synthetic data from multiple plants:
- Asset registry (machines/pumps/lines) and plant master data
- Time-series sensor readings (temperature, vibration, pressure, flow)
- Maintenance work orders (preventive/corrective)
- Quality inspection checks (pass/fail + defect codes)

Pipeline pattern:
ingest -> land to lake -> transform/validate -> publish curated tables -> monitoring + governance notes.

## Data lake zones
- lake/bronze: raw landed daily files (plants, assets, sensor_readings, work_orders, quality_inspections) + generation_meta.json
- lake/silver: validated sensor readings only (sensor_readings_clean.csv)
- lake/quarantine: rejected sensor readings (invalid or duplicate) with reject_reason codes (sensor_readings_rejects.csv)
- lake/gold: curated daily outputs derived from Silver and enriched with Bronze asset metadata (plant_kpis.csv, asset_health_daily.csv)

## Azure mapping (transferable)
- Orchestration (Azure Data Factory) -> GitHub Actions
- Data lake (ADLS Gen2) -> /lake (bronze/silver/gold/quarantine)
- Serving layer (Azure SQL / Synapse) -> Curated Gold CSV exports (used by Power BI / downstream tools)
- Secrets (Key Vault) -> GitHub Secrets (pattern)
- Monitoring (Azure Monitor / Log Analytics) -> Actions run history + logs + artifacts

## Outputs (generated per run date)
- Bronze: lake/bronze/YYYY-MM-DD/ (plants.csv, assets.csv, sensor_readings.jsonl, work_orders.csv, quality_inspections.csv, generation_meta.json)
- Silver: lake/silver/YYYY-MM-DD/sensor_readings_clean.csv
- Quarantine: lake/quarantine/YYYY-MM-DD/sensor_readings_rejects.csv
- Gold: lake/gold/YYYY-MM-DD/ (plant_kpis.csv, asset_health_daily.csv)
- Exports: exports/YYYY-MM-DD/plant_kpis.csv
- DQ report: reports/dq_YYYY-MM-DD.md

## Run the pipeline (GitHub Actions)

- Go to: Actions -> telemetry-pipeline-demo -> Run workflow
- Optional: set `run_date` as YYYY-MM-DD (UTC). Leave empty to use today's UTC date.
- Open the run:
  - The run Summary shows a short “Telemetry pipeline run summary”.
  - Outputs are provided as a downloadable Artifact ZIP.

## Download outputs (Artifacts)

- In the run page, scroll to **Artifacts** and download:
  - `pipeline-artifacts-YYYY-MM-DD.zip`

The ZIP contains:
- reports/dq_YYYY-MM-DD.md
  - Data Quality (DQ) report: counts + reject reasons from Bronze -> Silver
- lake/bronze/YYYY-MM-DD/
  - plants.csv, assets.csv, sensor_readings.jsonl, work_orders.csv, quality_inspections.csv, generation_meta.json
- lake/silver/YYYY-MM-DD/sensor_readings_clean.csv
  - Validated sensor readings (clean rows)
- lake/quarantine/YYYY-MM-DD/sensor_readings_rejects.csv
  - Rejected rows (invalid or duplicate) with reject_reason codes
- lake/gold/YYYY-MM-DD/plant_kpis.csv
- lake/gold/YYYY-MM-DD/asset_health_daily.csv
- exports/YYYY-MM-DD/plant_kpis.csv

Note:
- Generated data is not committed to git (keeps the repo clean).
- Use the artifact ZIP as the “evidence package” for each run.

