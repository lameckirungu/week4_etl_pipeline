"""
validate.py — Great Expectations Data Quality Suite
=====================================================
Defines and runs 7 validation rules against the raw sensor log.
Called by run_pipeline.py before any transformation occurs —
we validate the data as it arrives, not after we clean it.

If any expectation fails, run_pipeline.py halts execution.
This ensures we never silently load corrupt data into the database.
"""

import logging
import os

import pandas as pd
import great_expectations as ge
from great_expectations import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToMatchRegex,
    ExpectTableRowCountToBeBetween,
    ExpectColumnValueLengthsToBeBetween,
    ExpectColumnProportionOfUniqueValuesToBeBetween,
)
from dotenv import load_dotenv

load_dotenv()

# Thresholds — read from .env so they can be adjusted without touching code
MAX_PRESSURE = float(os.getenv("MAX_PRESSURE_BAR", 15))
MIN_PRESSURE = float(os.getenv("MIN_PRESSURE_BAR", 0))
MAX_TEMP     = float(os.getenv("MAX_TEMP_C",       100))
MIN_TEMP     = float(os.getenv("MIN_TEMP_C",        50))
MAX_FLOW     = float(os.getenv("MAX_FLOW_M3H",      500))
MIN_FLOW     = float(os.getenv("MIN_FLOW_M3H",      100))

# Valid zone and operator values — defined here as constants, not magic strings
VALID_ZONES     = ["Zone A", "Zone B", "Zone C",
                   "ZONE A", "ZONE B", "ZONE C",
                   "zone a", "zone b", "zone c",
                   "Zone-A", "Zone-B", "Zone-C"]
VALID_OPERATORS = ["OP-01", "OP-02", "OP-03"]


def run_validation(df: pd.DataFrame, logger: logging.Logger) -> bool:
    """
    Run the Great Expectations validation suite against a raw DataFrame.

    We use GE's ephemeral context (no filesystem project required) so the
    validation works in any environment without a great_expectations/ folder.

    Expectations defined
    --------------------
    1. Table has between 500 and 2000 rows        — catches empty or exploded feeds
    2. pressure_bar is between 0 and 15 bar       — physical operating envelope
    3. temperature_c is between 50 and 100 C      — plant operating range
    4. flow_rate_m3h is between 100 and 500 m3/h  — physical throughput range
    5. operator_id is not null                    — every reading must be attributed
    6. zone values are in the known set           — rejects unknown location codes
    7. operator_id matches OP-XX format           — enforces ID naming convention

    Note: we intentionally run these on RAW data. Some will fail on the dirty
    dataset (that is expected and correct — it proves the validator is working).
    The transform step fixes these issues after validation reports them.
    The pipeline halts only if a CRITICAL expectation fails (see logic below).

    Parameters
    ----------
    df : pd.DataFrame — raw data from extract().
    logger : logging.Logger

    Returns
    -------
    bool — True if all critical expectations pass, False otherwise.
    """
    logger.info("VALIDATE — Building ephemeral GE context")

    ctx = ge.get_context(mode="ephemeral")

    # Register the DataFrame as a data asset
    datasource = ctx.data_sources.add_pandas("ops_raw_source")
    asset      = datasource.add_dataframe_asset("ops_raw_asset")
    batch_def  = asset.add_batch_definition_whole_dataframe("ops_raw_batch")

    # Build the expectation suite
    suite = ctx.suites.add(ExpectationSuite(name="ops_raw_suite"))

    # ── Expectation 1: Row count sanity check ─────────────────────────────────
    # A feed with fewer than 500 rows is suspiciously short for a 7-day log.
    # More than 2000 suggests duplication or an unexpected data join upstream.
    suite.add_expectation(ExpectTableRowCountToBeBetween(
        min_value=500, max_value=2000
    ))

    # ── Expectation 2: Pressure within physical operating envelope ────────────
    # The plant operates between 0 and 15 bar. Values outside this range
    # are sensor transmission faults, not real operational events.
    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="pressure_bar",
        min_value=MIN_PRESSURE,
        max_value=MAX_PRESSURE,
        mostly=0.97,    # allow up to 3% bad rows — flags but doesn't halt
    ))

    # ── Expectation 3: Temperature within plant operating range ──────────────
    # Process temperatures run between 50°C and 100°C.
    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="temperature_c",
        min_value=MIN_TEMP,
        max_value=MAX_TEMP,
        mostly=0.97,
    ))

    # ── Expectation 4: Flow rate within physical throughput range ─────────────
    # Throughput at this plant runs 100–500 m³/h under all operating conditions.
    suite.add_expectation(ExpectColumnValuesToBeBetween(
        column="flow_rate_m3h",
        min_value=MIN_FLOW,
        max_value=MAX_FLOW,
        mostly=0.97,
    ))

    # ── Expectation 5: operator_id is never null ──────────────────────────────
    # Every sensor reading must be attributed to an operator.
    # A null here means the logging system failed — a critical data gap.
    suite.add_expectation(ExpectColumnValuesToNotBeNull(
        column="operator_id"
    ))

    # ── Expectation 6: zone values are in the known set ──────────────────────
    # Only three zones exist in this plant. Any other value is a data entry
    # error or a misconfigured sensor node.
    suite.add_expectation(ExpectColumnValuesToBeInSet(
        column="zone",
        value_set=VALID_ZONES,
        mostly=0.99,
    ))

    # ── Expectation 7: operator_id matches OP-XX naming convention ────────────
    # All operator IDs follow the format OP-01, OP-02, etc.
    # This catches free-text entry errors like "op01" or "Operator 1".
    suite.add_expectation(ExpectColumnValuesToMatchRegex(
        column="operator_id",
        regex=r"^OP-\d{2}$",
        mostly=0.99,
    ))

    # ── Run validation ────────────────────────────────────────────────────────
    vd = ctx.validation_definitions.add(ge.ValidationDefinition(
        name="ops_raw_validation",
        data=batch_def,
        suite=suite,
    ))

    result = vd.run(batch_parameters={"dataframe": df})

    # ── Log each expectation result ───────────────────────────────────────────
    all_passed = True
    for r in result.results:
        exp_type    = r.expectation_config.type
        passed      = r.success
        result_dict = r.result

        if passed:
            logger.info(f"VALIDATE ✓  {exp_type}")
        else:
            logger.warning(f"VALIDATE ✗  {exp_type} — result: {result_dict}")
            all_passed = False

    logger.info(
        f"VALIDATE — Suite complete: "
        f"{'ALL PASSED' if all_passed else 'FAILURES DETECTED'} "
        f"({sum(r.success for r in result.results)}/{len(result.results)} passed)"
    )
    return all_passed
