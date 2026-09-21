"""Sync the MySQL billing source into the PostgreSQL `raw` schema with dlt.

Write strategy is `replace`, not `merge`: dlt's merge deduplicates on the primary key,
which would collapse the planted duplicates during ingestion. `replace` reloads each run,
so raw mirrors the source exactly and reruns stay idempotent.

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

TABLES = {
    "customers": "customer_id",
    "subscriptions": "subscription_id",
    "invoices": "invoice_id",
}

log = logging.getLogger("sync_mysql_to_postgres")


def load_dotenv(path: Path) -> None:
    """Minimal .env loader. Existing environment variables win."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def mysql_url() -> str:
    """Source connection string."""
    return (
        f"mysql+pymysql://{os.environ.get('MYSQL_USER')}"
        f":{os.environ.get('MYSQL_PASSWORD')}"
        f"@{os.environ.get('MYSQL_HOST')}"
        f":{os.environ.get('MYSQL_PORT')}"
        f"/{os.environ.get('MYSQL_DATABASE')}"
    )


def postgres_url() -> str:
    """Destination connection string."""
    return (
        f"postgresql://{os.environ.get('POSTGRES_USER')}"
        f":{os.environ.get('POSTGRES_PASSWORD')}"
        f"@{os.environ.get('POSTGRES_HOST')}"
        f":{os.environ.get('POSTGRES_PORT')}"
        f"/{os.environ.get('POSTGRES_DB')}"
    )


def build_source():
    """The three billing tables, each set to replace and keyed by its business key."""
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

    Distinct-key parity is the one that catches silent deduplication.
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

    # dlt records failed jobs but still exits 0.
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
