"""
run_pipeline.py — Week 4 ETL Pipeline
======================================
Modular Extract → Validate → Transform → Load pipeline for the
KPC Fuel Processing Plant sensor log.

Usage:
    python run_pipeline.py                  # run full pipeline
    python run_pipeline.py --dry-run        # extract + validate only, no load
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from validate import run_validation

# ── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()                                   # reads .env into os.environ

RAW_CSV      = os.getenv("RAW_CSV_PATH",      "data/ops_sensor_log_dirty.csv")
OUTPUT_DB    = os.getenv("OUTPUT_DB_PATH",    "data/ops_pipeline.db")
OUTPUT_TABLE = os.getenv("OUTPUT_TABLE",      "cleaned_sensor_log")
LOG_FILE     = os.getenv("LOG_FILE",          "pipeline.log")
MAX_PRESSURE = float(os.getenv("MAX_PRESSURE_BAR", 15))
MIN_PRESSURE = float(os.getenv("MIN_PRESSURE_BAR", 0))
MAX_TEMP     = float(os.getenv("MAX_TEMP_C",       100))
MIN_TEMP     = float(os.getenv("MIN_TEMP_C",        50))
MAX_FLOW     = float(os.getenv("MAX_FLOW_M3H",      500))
MIN_FLOW     = float(os.getenv("MIN_FLOW_M3H",      100))

# ── Logging setup ──────────────────────────────────────────────────────────────
def setup_logging(log_file: str) -> logging.Logger:
    """
    Configure logging to write to both a file and the console simultaneously.
    File captures the permanent audit trail; console gives real-time feedback.
    """
    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler — append so historical runs are preserved
    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Console handler — INFO and above only
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ── EXTRACT ───────────────────────────────────────────────────────────────────
def extract(csv_path: str, logger: logging.Logger) -> pd.DataFrame:
    """
    Load the raw CSV from disk.

    No transformations here — the extract step is intentionally dumb.
    Its only job is to get the data into memory and report what it found.
    Separating extract from transform means we can swap the source
    (e.g., to an API or database) without touching the cleaning logic.

    Parameters
    ----------
    csv_path : str
        Path to the raw sensor log CSV file.
    logger : logging.Logger

    Returns
    -------
    pd.DataFrame — raw, untouched data as it arrived from the source.
    """
    logger.info(f"EXTRACT — Reading from: {csv_path}")
    path = Path(csv_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Source file not found: {csv_path}\n"
            f"Place ops_sensor_log_dirty.csv in the data/ directory."
        )

    df = pd.read_csv(csv_path)
    logger.info(f"EXTRACT — Loaded {len(df):,} rows × {df.shape[1]} columns")
    logger.debug(f"EXTRACT — Columns: {list(df.columns)}")
    logger.debug(f"EXTRACT — Raw null counts:\n{df.isnull().sum().to_dict()}")
    return df


# ── TRANSFORM ─────────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Clean and enrich the raw sensor log.

    All five quality issues identified in Week 2 are handled here.
    This function is idempotent — running it twice on the same input
    produces the same output. Safe to call in scheduled pipelines.

    Steps
    -----
    1. Parse timestamps; drop unrecoverable rows.
    2. Sort chronologically.
    3. Remove duplicate rows.
    4. Standardise zone label formatting.
    5. Null-out physically impossible sensor readings.
    6. Linearly interpolate remaining nulls.
    7. Assign shift labels (Morning / Afternoon / Night).
    8. Add pipeline metadata columns (load_timestamp, pipeline_version).

    Parameters
    ----------
    df : pd.DataFrame — raw data from extract().
    logger : logging.Logger

    Returns
    -------
    pd.DataFrame — fully cleaned and enriched DataFrame.
    """
    logger.info("TRANSFORM — Starting cleaning pipeline")
    df = df.copy()
    n_start = len(df)

    # Step 1 — Parse timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    n_bad_ts = df["timestamp"].isna().sum()
    df = df.dropna(subset=["timestamp"])
    logger.info(f"TRANSFORM — Step 1: dropped {n_bad_ts} unparseable timestamp rows")

    # Step 2 — Sort chronologically
    df = df.sort_values("timestamp").reset_index(drop=True)

    # Step 3 — Remove duplicates
    n_before = len(df)
    df = df.drop_duplicates()
    n_dupes = n_before - len(df)
    logger.info(f"TRANSFORM — Step 3: removed {n_dupes} duplicate rows")

    # Step 4 — Standardise zone labels
    df["zone"] = (
        df["zone"]
        .str.strip()
        .str.upper()
        .str.replace(r"[-_]", " ", regex=True)
    )
    logger.info(f"TRANSFORM — Step 4: zone labels → {sorted(df['zone'].unique())}")

    # Step 5 — Null-out impossible sensor readings
    impossible_p = (df["pressure_bar"] < MIN_PRESSURE) | (df["pressure_bar"] > MAX_PRESSURE)
    impossible_t = (df["temperature_c"] < MIN_TEMP)    | (df["temperature_c"] > MAX_TEMP)
    impossible_f = (df["flow_rate_m3h"] < MIN_FLOW)    | (df["flow_rate_m3h"] > MAX_FLOW)
    df.loc[impossible_p, "pressure_bar"]  = np.nan
    df.loc[impossible_t, "temperature_c"] = np.nan
    df.loc[impossible_f, "flow_rate_m3h"] = np.nan
    logger.info(
        f"TRANSFORM — Step 5: nulled impossible readings — "
        f"pressure={impossible_p.sum()}, temp={impossible_t.sum()}, flow={impossible_f.sum()}"
    )

    # Step 6 — Interpolate
    sensor_cols = ["pressure_bar", "temperature_c", "flow_rate_m3h"]
    for col in sensor_cols:
        n_null = df[col].isna().sum()
        df[col] = df[col].interpolate(method="linear", limit_direction="both")
        if n_null > 0:
            logger.info(f"TRANSFORM — Step 6: interpolated {n_null} nulls in '{col}'")

    # Step 7 — Shift labels
    def _shift(hour: int) -> str:
        if 6 <= hour < 14:   return "Morning"
        elif 14 <= hour < 22: return "Afternoon"
        else:                  return "Night"

    df["shift"] = df["timestamp"].dt.hour.map(_shift)

    # Step 8 — Pipeline metadata
    df["load_timestamp"]    = datetime.utcnow().isoformat()
    df["pipeline_version"]  = "1.0.0"

    n_end = len(df)
    logger.info(
        f"TRANSFORM — Complete: {n_start} rows in → {n_end} rows out "
        f"({n_start - n_end} dropped total)"
    )
    logger.debug(f"TRANSFORM — Final null check: {df.isnull().sum().sum()} nulls")
    return df


# ── LOAD ──────────────────────────────────────────────────────────────────────
def load(df: pd.DataFrame, db_path: str, table: str, logger: logging.Logger) -> None:
    """
    Write the cleaned DataFrame to a SQLite database table.

    Idempotency strategy: DROP and recreate the table on every run.
    This guarantees the database always reflects the latest successful
    pipeline execution — no duplicate rows accumulate across runs.

    An alternative idempotency strategy (upsert on unique timestamp)
    is noted in the README for production use cases where history must
    be preserved.

    Parameters
    ----------
    df : pd.DataFrame — cleaned data from transform().
    db_path : str — path to the target SQLite database file.
    table : str — target table name.
    logger : logging.Logger
    """
    logger.info(f"LOAD — Target: {db_path} → table '{table}'")

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}", echo=False)

    with engine.begin() as conn:
        # Idempotency — clear before reload
        conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
        logger.info(f"LOAD — Dropped existing '{table}' table (idempotent reset)")

    df.to_sql(table, engine, if_exists="replace", index=False)
    logger.info(f"LOAD — Wrote {len(df):,} rows to '{table}'")

    # Verify row count in DB matches what we wrote
    with engine.connect() as conn:
        db_count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    if db_count != len(df):
        raise RuntimeError(
            f"Row count mismatch: wrote {len(df)}, DB has {db_count}"
        )
    logger.info(f"LOAD — Verified: {db_count:,} rows confirmed in database ✓")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main(dry_run: bool = False) -> None:
    logger = setup_logging(LOG_FILE)
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info(f"PIPELINE START — run_id={run_id}  dry_run={dry_run}")
    logger.info("=" * 60)
    start = datetime.utcnow()

    try:
        # ── Extract ───────────────────────────────────────────────
        df_raw = extract(RAW_CSV, logger)

        # ── Validate raw data ─────────────────────────────────────
        logger.info("VALIDATE — Running Great Expectations suite on raw data")
        validation_passed = run_validation(df_raw, logger)
        if not validation_passed:
            logger.error(
                "VALIDATE — One or more expectations failed. "
                "Pipeline halted. Inspect pipeline.log for details."
            )
            sys.exit(1)
        logger.info("VALIDATE — All expectations passed ✓")

        # ── Transform ─────────────────────────────────────────────
        df_clean = transform(df_raw, logger)

        # ── Load ─────────────────────────────────────────────────
        if dry_run:
            logger.info("DRY RUN — Skipping load step. Pipeline complete.")
        else:
            load(df_clean, OUTPUT_DB, OUTPUT_TABLE, logger)

    except FileNotFoundError as e:
        logger.error(f"FILE ERROR — {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"UNEXPECTED ERROR — {type(e).__name__}: {e}")
        sys.exit(1)
    finally:
        elapsed = (datetime.utcnow() - start).total_seconds()
        logger.info(f"PIPELINE END — run_id={run_id}  elapsed={elapsed:.2f}s")
        logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KPC Ops ETL Pipeline")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run extract + validate only. Do not write to database."
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
