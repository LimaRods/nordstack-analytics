"""Bootstrap the simulated MySQL billing source from seed_data/*.csv.

This initializes the fictional source system; the analytics ingestion pipeline is
sync_mysql_to_postgres.py.

Idempotent by replacement: the DDL drops and recreates each table, so running it twice
leaves 121 / 175 / 2855 rows. Every planted defect is preserved -- the only change
applied is converting empty CSV strings to NULL for the three nullable columns.

Usage:
    python ingestion/bootstrap_mysql.py
"""

from __future__ import annotations

import csv
import logging
import os
import sys
from pathlib import Path

import pymysql

REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_DIR = REPO_ROOT / "seed_data"
DDL_PATH = Path(__file__).resolve().parent / "ddl" / "mysql_source.sql"

# table -> (csv file, columns in insert order, columns where '' means NULL)
TABLES = {
    "customers": (
        "raw_customers.csv",
        ["customer_id", "customer_name", "email", "country", "created_at"],
        {"country"},
    ),
    "subscriptions": (
        "raw_subscriptions.csv",
        [
            "subscription_id",
            "customer_id",
            "plan_name",
            "monthly_price",
            "start_date",
            "end_date",
            "status",
        ],
        {"end_date"},
    ),
    "invoices": (
        "raw_invoices.csv",
        ["invoice_id", "subscription_id", "invoice_date", "amount", "currency", "status"],
        {"amount"},
    ),
}

# Row counts including the planted duplicates. The bootstrap must reproduce the
# source extract exactly, so these are raw row counts, not distinct key counts.
EXPECTED_ROWS = {"customers": 121, "subscriptions": 175, "invoices": 2855}

log = logging.getLogger("bootstrap_mysql")


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


def connect() -> pymysql.connections.Connection:
    """Connect using environment configuration. No credentials in source."""
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "billing_user"),
        password=os.environ.get("MYSQL_PASSWORD", "billing_password"),
        database=os.environ.get("MYSQL_DATABASE", "billing"),
        charset="utf8mb4",
        autocommit=False,
    )


def read_csv(path: Path, columns: list[str], nullable: set[str]) -> list[tuple]:
    """Read a CSV into insert tuples, preserving every value verbatim.

    The only conversion is '' -> None for genuinely nullable columns, so the
    database stores NULL rather than an empty string. Trailing whitespace and
    casing are left untouched: 'PAID ' must survive to the staging layer.
    """
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        missing = set(columns) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name} is missing expected columns: {sorted(missing)}")
        rows = [
            tuple(
                None if (col in nullable and row[col].strip() == "") else row[col]
                for col in columns
            )
            for row in reader
        ]
    return rows


def execute_ddl(cursor) -> None:
    """Run the DDL, dropping and recreating every source table.

    Comments are stripped before splitting on ';' -- the DDL's own comments contain
    semicolons, which would otherwise cut statements in half.
    """
    lines = [
        line for line in DDL_PATH.read_text().splitlines() if not line.strip().startswith("--")
    ]
    for statement in (s.strip() for s in "\n".join(lines).split(";")):
        if statement:
            cursor.execute(statement)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_dotenv(REPO_ROOT / ".env")

    # Fail before touching the database if any input is missing.
    if not DDL_PATH.exists():
        log.error("DDL not found: %s", DDL_PATH)
        return 1
    for table, (filename, _, _) in TABLES.items():
        if not (SEED_DIR / filename).exists():
            log.error("Required seed file missing for %s: %s", table, SEED_DIR / filename)
            return 1

    connection = connect()
    try:
        with connection.cursor() as cursor:
            log.info("Applying DDL from %s", DDL_PATH.relative_to(REPO_ROOT))
            execute_ddl(cursor)

            for table, (filename, columns, nullable) in TABLES.items():
                rows = read_csv(SEED_DIR / filename, columns, nullable)
                placeholders = ", ".join(["%s"] * len(columns))
                cursor.executemany(
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                    rows,
                )
                log.info("Loaded %-14s %5d rows from %s", table, len(rows), filename)

        connection.commit()

        # Verify against the source extract; a mismatch means rows were lost or doubled.
        failures = []
        with connection.cursor() as cursor:
            for table, expected in EXPECTED_ROWS.items():
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                actual = cursor.fetchone()[0]
                if actual != expected:
                    failures.append(f"{table}: expected {expected} rows, found {actual}")
                else:
                    log.info("Verified %-14s %5d rows", table, actual)

        if failures:
            for failure in failures:
                log.error(failure)
            return 1

    except Exception:
        connection.rollback()
        log.exception("Bootstrap failed; no changes committed")
        return 1
    finally:
        connection.close()

    log.info("MySQL source bootstrap complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
