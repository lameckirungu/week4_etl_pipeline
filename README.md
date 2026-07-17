# Week 4 — Automated ETL Pipeline
### KPC Fuel Processing Plant · Operational Sensor Log

**Analyst:** Lameck Irungu · KPC Cohort, Inuka Fellowship

---

## What This Does

An end-to-end **Extract → Validate → Transform → Load** pipeline that:

1. Reads a raw sensor log CSV from disk
2. Validates it against 7 data quality rules using Great Expectations
3. Cleans and enriches the data (timestamps, duplicates, outliers, shift labels)
4. Loads the clean result into a SQLite database
5. Logs every step with timestamps and row counts to `pipeline.log`

The pipeline is **idempotent** — running it multiple times produces the same
result. The target table is dropped and recreated on every run.

---

## Repository Structure

```
week4_etl_pipeline/
├── run_pipeline.py        ← Entry point — orchestrates E, V, T, L
├── validate.py            ← Great Expectations suite (7 rules)
├── .env.example           ← Configuration template (copy → .env)
├── .gitignore
├── requirements.txt
├── hackathon_reflection.md
├── cron_setup.txt         ← Automation proof (cron / Task Scheduler)
├── README.md
└── data/
    └── ops_sensor_log_dirty.csv   ← Raw source file (place here)
```

---

## Installation

```bash
# 1. Clone the repo
git clone https://github.com/lameckirungu/week4_etl_pipeline.git
cd week4_etl_pipeline

# 2. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set up configuration
cp .env.example .env
# Edit .env if you need to change any paths or thresholds
```

---

## Setup

Place the raw data file in the `data/` directory:

```
data/
└── ops_sensor_log_dirty.csv
```

The `data/` directory is created automatically if it does not exist.
The output database (`data/ops_pipeline.db`) is generated on first run.

---

## Running the Pipeline

```bash
# Full pipeline — extract, validate, transform, load
python run_pipeline.py

# Dry run — extract and validate only, no database write
python run_pipeline.py --dry-run
```

### Expected output (console)

```
2026-06-10 08:00:01 | INFO     | PIPELINE START — run_id=20260610_080001
2026-06-10 08:00:01 | INFO     | EXTRACT — Reading from: data/ops_sensor_log_dirty.csv
2026-06-10 08:00:01 | INFO     | EXTRACT — Loaded 1,018 rows × 6 columns
2026-06-10 08:00:02 | INFO     | VALIDATE — All expectations passed ✓
2026-06-10 08:00:02 | INFO     | TRANSFORM — Complete: 1,018 rows in → 1,002 rows out
2026-06-10 08:00:02 | INFO     | LOAD — Wrote 1,002 rows to 'cleaned_sensor_log'
2026-06-10 08:00:02 | INFO     | LOAD — Verified: 1,002 rows confirmed in database ✓
2026-06-10 08:00:02 | INFO     | PIPELINE END — elapsed=1.23s
```

---

## Configuration (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `RAW_CSV_PATH` | `data/ops_sensor_log_dirty.csv` | Path to raw input file |
| `OUTPUT_DB_PATH` | `data/ops_pipeline.db` | Path to output SQLite database |
| `OUTPUT_TABLE` | `cleaned_sensor_log` | Target table name |
| `LOG_FILE` | `pipeline.log` | Log file path |
| `MAX_PRESSURE_BAR` | `15` | Upper pressure threshold (bar) |
| `MIN_PRESSURE_BAR` | `0` | Lower pressure threshold (bar) |
| `MAX_TEMP_C` | `100` | Upper temperature threshold (°C) |
| `MIN_TEMP_C` | `50` | Lower temperature threshold (°C) |
| `MAX_FLOW_M3H` | `500` | Upper flow rate threshold (m³/h) |
| `MIN_FLOW_M3H` | `100` | Lower flow rate threshold (m³/h) |

---

## Data Quality Rules (Great Expectations)

| # | Expectation | Threshold |
|---|-------------|-----------|
| 1 | Table row count between 500 and 2,000 | Critical |
| 2 | `pressure_bar` between 0 and 15 bar | 97% of rows |
| 3 | `temperature_c` between 50 and 100 °C | 97% of rows |
| 4 | `flow_rate_m3h` between 100 and 500 m³/h | 97% of rows |
| 5 | `operator_id` is never null | 100% of rows |
| 6 | `zone` is one of: Zone A, Zone B, Zone C | 99% of rows |
| 7 | `operator_id` matches regex `^OP-\d{2}$` | 99% of rows |

If any expectation fails, the pipeline halts before writing to the database.

---

## Automation

See `cron_setup.txt` for the cron job (Linux/macOS) and Windows Task
Scheduler configuration to run the pipeline daily at 06:00.

---

## Idempotency

The load step drops and recreates the target table on every run.
This means:
- Running the pipeline twice gives the same result as running it once
- No duplicate rows accumulate across scheduled executions
- The database always reflects the most recent successful run

For production use cases where historical loads must be preserved, the
README notes an alternative upsert strategy keyed on `timestamp`.
