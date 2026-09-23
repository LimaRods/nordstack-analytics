"""NordStack analytics pipeline: dlt_sync -> validate_raw -> dbt_build.

Runs every five minutes and emails on success and failure. Three tasks rather than one
script, so a failure says which layer it came from and a retry only repeats that task.
Every task is idempotent, so retries are safe; they are bounded at two retries.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

REPO_ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from nordstack.config import VALID_TARGETS, environment_default, resolve_dbt_target  # noqa: E402
from nordstack.notifications import (  # noqa: E402
    notify_failure,
    notify_sla_miss,
    notify_success,
)

DBT_DIR = REPO_ROOT / "dbt"
DBT_BIN = REPO_ROOT / "venv" / "bin" / "dbt"


def _run(callable_returning_exit_code, label: str) -> None:
    """Call a script's main() and raise on a non-zero exit code.

    PythonOperator only fails a task if the callable raises, and both ingestion CLIs
    return an exit code instead.
    """
    exit_code = callable_returning_exit_code()
    if exit_code != 0:
        raise RuntimeError(f"{label} failed with exit code {exit_code}")


def sync_mysql_to_postgres(**_) -> None:
    from ingestion.sync_mysql_to_postgres import main

    _run(main, "dlt sync")


def validate_raw(**_) -> None:
    """Re-assert the invariants after ingestion, independently of the loader."""
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


def dbt_build(**context) -> None:
    """Run `dbt build`: a failing test skips that model's dependents, so bad data stops
    where it appears. The target comes from the run, not from this file.
    """
    import logging
    import subprocess

    log = logging.getLogger(__name__)
    target = resolve_dbt_target(context)
    log.info("Running dbt build against target=%s", target)

    task_instance = context.get("task_instance")
    if task_instance is not None:
        task_instance.xcom_push(key="dbt_target", value=target)

    result = subprocess.run(
        [str(DBT_BIN), "build", "--target", target],
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
    )
    log.info(result.stdout)
    if result.stderr:
        log.warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"dbt build failed (exit {result.returncode}) against {target}")


default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
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
    # Chosen per run on the trigger form; defaults to dev, never to prod.
    params={
        "dbt_target": Param(
            default=environment_default(),
            enum=list(VALID_TARGETS),
            type="string",
            title="dbt target",
            description=(
                "Schema to build into. `dev` for development and testing; select "
                "`prod` only for a deliberate production build."
            ),
        )
    },
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
    sla_miss_callback=notify_sla_miss,
    tags=["nordstack", "dbt", "dlt"],
    doc_md=__doc__,
) as dag:

    sync = PythonOperator(
        task_id="dlt_sync",
        python_callable=sync_mysql_to_postgres,
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
