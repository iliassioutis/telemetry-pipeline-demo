# Industrial operations & maintenance data pipeline (synthetic)

This repo demonstrates an "Azure-style" data pipeline using free tooling, designed to mirror a typical
manufacturing / industrial analytics scenario (multi-plant data consolidation).

## Scenario
Daily ingest of synthetic data from multiple plants:
- Asset registry (machines/pumps/lines) and plant master data
- Time-series sensor readings (temperature, vibration, pressure, flow)
- Maintenance work orders (preventive/corrective)
- Quality inspection checks (pass/fail + defect codes)

Pipeline pattern:
ingest -> land to lake -> transform/validate -> publish curated tables -> monitoring + governance notes.

## Data lake zones
- lake/bronze: raw landed files (as received)
- lake/silver: cleaned/standardized/conformed data
- lake/gold: curated “business-ready” tables/exports
- lake/quarantine: rejected records with reason codes

## Azure mapping (transferable)
- Orchestration (Azure Data Factory) -> GitHub Actions
- Data lake (ADLS Gen2) -> /lake (bronze/silver/gold/quarantine)
- Serving layer (Azure SQL / Synapse) -> DuckDB + exported CSV/Parquet
- Secrets (Key Vault) -> GitHub Secrets (pattern)
- Monitoring (Azure Monitor / Log Analytics) -> Actions run history + logs + artifacts

## Outputs (to be generated)
- Synthetic daily batch files in lake/bronze/
- Curated tables in lake/gold/ and exports/
- Data quality report in reports/
