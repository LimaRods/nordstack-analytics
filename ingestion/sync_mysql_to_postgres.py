"""Sync the MySQL billing source into the PostgreSQL `raw` schema with dlt.

This is the analytics ingestion pipeline. It runs on every Airflow schedule
(every 5 minutes) and must be safe to retry.

Write strategy: `replace`, not `merge`.
---------------------------------------
`merge` would be the obvious choice for idempotency, but dlt's merge job
deduplicates on the primary key (ROW_NUMBER() OVER (PARTITION BY ...) in
SqlMergeFollowupJob). That would silently collapse the planted duplicate rows
C0023 and S00006 during ingestion, and CANDIDATE_BRIEF.md section 3 requires the
raw/staging tests to *catch* those defects.

`replace` reloads the table each run, so:
  - raw mirrors the source exactly, defects included (121 / 175 / 2855);
  - re-running is still idempotent -- counts never accumulate;
  - deduplication moves to dbt staging, where it is visible and tested.

The business primary key is still declared as a resource hint. dlt maps only the
`unique` hint to a Postgres constraint, so declaring `primary_key` documents the
key and marks it NOT NULL without rejecting the duplicates.

Usage:
    python ingestion/sync_mysql_to_postgres.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import dlt
from dlt.sources.sql_database import sql_database

REPO_ROOT = Path(__file__).resolve().parent.parent

PIPELINE_NAME = "nordstack_billing"
DATASET_NAME = "raw"

# Only these tables are ingested. The business key is declared per table so the
# schema documents it; see the module docstring for why it does not dedupe.
TABLES = {
    "customers": "customer_id",
    "subscriptions": "subscription_id",
    "invoices": "invoice_id",
}

log = logging.getLogger("sync_mysql_to_postgres")


def load_dotenv(path: Path) -> None:
    """Minimal .env loader. Existing environment variables always win."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def mysql_url() -> str:
    """Source connection string, built from environment configuration."""
    return (
        f"mysql+pymysql://{os.environ.get('MYSQL_USER', 'billing_user')}"
        f":{os.environ.get('MYSQL_PASSWORD', 'billing_password')}"
        f"@{os.environ.get('MYSQL_HOST', '127.0.0.1')}"
        f":{os.environ.get('MYSQL_PORT', '3306')}"
        f"/{os.environ.get('MYSQL_DATABASE', 'billing')}"
    )


def postgres_url() -> str:
    """Destination connection string, built from environment configuration."""
    return (
        f"postgresql://{os.environ.get('POSTGRES_USER', 'dbt_user')}"
        f":{os.environ.get('POSTGRES_PASSWORD', 'dbt_password')}"
        f"@{os.environ.get('POSTGRES_HOST', '127.0.0.1')}"
        f":{os.environ.get('POSTGRES_PORT', '5432')}"
        f"/{os.environ.get('POSTGRES_DB', 'analytics')}"
    )


def build_source():
    """The three billing tables, each set to replace and keyed by its business PK."""
    source = sql_database(
        credentials=mysql_url(),
        table_names=list(TABLES),
        reflection_level="full",
    )
    for table, primary_key in TABLES.items():
        source.resources[table].apply_hints(
            primary_key=primary_key,
            write_disposition="replace",
        )
    return source


def validate(pipeline) -> list[str]:
    """Check the load against the source. Returns a list of failures.

    Two invariants, both required:
      - row-count parity  -> nothing was lost or double-loaded;
      - distinct-key parity -> nothing was silently deduplicated.

    The second is the one that would catch a regression back to `merge`.
    """
    failures: list[str] = []
    from sqlalchemy import create_engine, text

    source_engine = create_engine(mysql_url())
    with pipeline.sql_client() as client, source_engine.connect() as source:
        for table, primary_key in TABLES.items():
            qualified = client.make_qualified_table_name(table)

            try:
                rows = client.execute_sql(f"SELECT COUNT(*) FROM {qualified}")[0][0]
                keys = client.execute_sql(
                    f"SELECT COUNT(DISTINCT {primary_key}) FROM {qualified}"
                )[0][0]
                nulls = client.execute_sql(
                    f"SELECT COUNT(*) FROM {qualified} WHERE {primary_key} IS NULL"
                )[0][0]
            except Exception as exc:  # table missing or unreadable
                failures.append(f"{table}: destination table not queryable ({exc})")
                continue

            src = source.execute(
                text(f"SELECT COUNT(*), COUNT(DISTINCT {primary_key}) FROM {table}")
            ).fetchone()
            src_rows, src_keys = int(src[0]), int(src[1])

            if rows != src_rows:
                failures.append(
                    f"{table}: row count mismatch -- source {src_rows}, destination {rows}"
                )
            if keys != src_keys:
                failures.append(
                    f"{table}: distinct-key mismatch -- source {src_keys}, destination {keys}"
                    " (rows were deduplicated during ingestion)"
                )
            if nulls:
                failures.append(f"{table}: {nulls} rows have a NULL {primary_key}")

            duplicates = rows - keys
            log.info(
                "Verified %-14s %5d rows, %5d distinct %s%s",
                table,
                rows,
                keys,
                primary_key,
                f" ({duplicates} duplicate row(s) preserved)" if duplicates else "",
            )
    source_engine.dispose()
    return failures


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_dotenv(REPO_ROOT / ".env")

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.postgres(credentials=postgres_url()),
        dataset_name=DATASET_NAME,
    )

    log.info("Syncing %s -> postgres.%s", ", ".join(TABLES), DATASET_NAME)
    load_info = pipeline.run(build_source())
    log.info("%s", load_info)

    # Surface failed dlt jobs as a process failure so Airflow marks the task failed
    # and applies its retry policy. Never swallow a partial load.
    load_info.raise_on_failed_jobs()

    failures = validate(pipeline)
    if failures:
        for failure in failures:
            log.error(failure)
        return 1

    log.info("Ingestion complete and validated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
