"""NordStack analytics pipeline.

    dlt_sync  ->  validate_raw  ->  dbt_build

Runs every five minutes, per the assessment brief, and emails on both success and
failure.

WHY THESE THREE TASKS AND NOT ONE
Running the whole thing as a single script would be simpler and strictly worse. Split
this way, a failure names itself: a red `dlt_sync` is an ingestion problem, a red
`validate_raw` means ingestion reported success but the warehouse disagrees, and a red
`dbt_build` is a modelling or data-quality problem. Airflow also retries only the task
that failed rather than redoing work that already succeeded.

WHAT MAKES RETRIES SAFE
Every task is idempotent, so a retry repeats work rather than compounding it:
  - dlt uses write_disposition="replace" -- reloads, never appends. Two runs leave
    121/175/2855 rows, never 242/350/5710.
  - validate_raw only reads.
  - dbt views and tables are rebuilt in full (CREATE OR REPLACE / DROP+CREATE).
This is what lets retries be a recovery mechanism instead of a duplication risk.

TRANSIENT VS DETERMINISTIC
Retries fix dropped connections and container restarts. They do not fix broken SQL, a
schema change, or a failing data-quality test -- those fail identically every time, so
the policy is bounded (2 attempts) rather than infinite. Exhausted retries send the
failure email instead of quietly consuming the error.

ENVIRONMENT
Connection settings, SMTP credentials and DBT_TARGET all come from the environment.
DBT_TARGET falls back to `dev`, so a DAG tested locally cannot write to production.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

REPO_ROOT = Path(__file__).resolve().parents[2]

# The ingestion modules are imported rather than shelled out to, so a failure surfaces
# as a Python exception with a real traceback in the task log.
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from nordstack.notifications import (  # noqa: E402
    notify_failure,
    notify_sla_miss,
    notify_success,
)

DBT_DIR = REPO_ROOT / "dbt"
DBT_BIN = REPO_ROOT / "venv" / "bin" / "dbt"


def _run(callable_returning_exit_code, label: str) -> None:
    """Call a script's main() and RAISE on a non-zero exit code.

    This adapter exists for a specific trap: PythonOperator marks a task successful
    unless it raises. A main() that *returns* 1 -- as both ingestion scripts do, being
    CLI programs -- would be reported as a green task holding a failed load. Converting
    the exit code into an exception is what makes the task state honest.
    """
    exit_code = callable_returning_exit_code()
    if exit_code != 0:
        raise RuntimeError(f"{label} failed with exit code {exit_code}")


def sync_mysql_to_postgres(**_) -> None:
    from ingestion.sync_mysql_to_postgres import main

    _run(main, "dlt sync")


def validate_raw(**_) -> None:
    """Re-assert the invariants after ingestion, independently of the loader.

    The sync script already validates its own work. Repeating the check here is
    deliberate: a loader that reports success while leaving the warehouse wrong is
    exactly the failure a self-check cannot catch, and this task fails the run before
    dbt builds anything on top of it.
    """
    import logging

    from sqlalchemy import create_engine, text

    from ingestion.sync_mysql_to_postgres import TABLES, load_dotenv, mysql_url, postgres_url

    log = logging.getLogger(__name__)
    load_dotenv(REPO_ROOT / ".env")

    source = create_engine(mysql_url())
    destination = create_engine(postgres_url())
    problems: list[str] = []

    with source.connect() as src, destination.connect() as dst:
        for table, primary_key in TABLES.items():
            src_rows, src_keys = src.execute(
                text(f"SELECT COUNT(*), COUNT(DISTINCT {primary_key}) FROM {table}")
            ).fetchone()
            dst_rows, dst_keys, nulls = dst.execute(
                text(
                    f"SELECT COUNT(*), COUNT(DISTINCT {primary_key}), "
                    f"COUNT(*) FILTER (WHERE {primary_key} IS NULL) FROM raw.{table}"
                )
            ).fetchone()

            if src_rows != dst_rows:
                problems.append(f"{table}: {src_rows} rows at source, {dst_rows} in raw")
            # Distinct-key parity is the check that matters most: row counts alone would
            # still pass if rows were deduplicated and duplicated in equal measure, and
            # silent deduplication is precisely what `replace` exists to prevent.
            if src_keys != dst_keys:
                problems.append(
                    f"{table}: {src_keys} distinct {primary_key} at source, {dst_keys} in raw "
                    f"-- rows were deduplicated during ingestion"
                )
            if nulls:
                problems.append(f"{table}: {nulls} rows with a NULL {primary_key}")

            log.info("%-14s %5d rows, %5d distinct %s", table, dst_rows, dst_keys, primary_key)

    source.dispose()
    destination.dispose()

    if problems:
        raise ValueError("Raw layer failed validation: " + "; ".join(problems))


def dbt_build(**_) -> None:
    """Run `dbt build` -- models and tests interleaved, in dependency order.

    `build` rather than `run` then `test`: build tests each model immediately after
    creating it and skips that model's dependents if a test fails, so bad data stops
    where it appears instead of propagating into the marts before anything notices.
    """
    import logging
    import subprocess

    log = logging.getLogger(__name__)
    target = os.environ.get("DBT_TARGET", "dev")
    log.info("Running dbt build against target=%s", target)

    result = subprocess.run(
        [str(DBT_BIN), "build", "--target", target],
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
    )
    # dbt's own output is the diagnostic. Put it in the task log before deciding.
    log.info(result.stdout)
    if result.stderr:
        log.warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"dbt build failed (exit {result.returncode}) against {target}")


default_args = {
    "owner": "analytics",
    # Bounded, never infinite. Two attempts recover a dropped connection; a broken
    # model fails identically every time and should surface, not spin.
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    # A task that outlives the schedule interval is stuck, not slow.
    "execution_timeout": timedelta(minutes=4),
}

with DAG(
    dag_id="nordstack_analytics",
    description="MySQL -> dlt -> PostgreSQL raw -> dbt marts (MRR, LTV, churn)",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,          # nothing is gained by backfilling five-minute intervals
    max_active_runs=1,      # two runs replacing the same tables would race each other
    dagrun_timeout=timedelta(minutes=4),   # under the interval, so a stuck run cannot pile up
    default_args=default_args,
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
    sla_miss_callback=notify_sla_miss,
    tags=["nordstack", "dbt", "dlt"],
    doc_md=__doc__,
) as dag:

    sync = PythonOperator(
        task_id="dlt_sync",
        python_callable=sync_mysql_to_postgres,
        # SLA is per-task and measured from the DAG run start: lateness here is the
        # earliest signal that the 5-minute cadence is at risk.
        sla=timedelta(minutes=2),
        doc_md="Extract from MySQL, normalize, load into PostgreSQL `raw` with "
               "write_disposition=replace. Idempotent: reloads rather than appends.",
    )

    validate = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw,
        sla=timedelta(minutes=3),
        doc_md="Independent post-load check: row-count parity, distinct-key parity "
               "(catches silent deduplication) and non-null business keys.",
    )

    build = PythonOperator(
        task_id="dbt_build",
        python_callable=dbt_build,
        sla=timedelta(minutes=4),
        doc_md="`dbt build` against the target named by DBT_TARGET (default `dev`). "
               "Models and tests interleaved, so a failing test stops its dependents.",
    )

    sync >> validate >> build
