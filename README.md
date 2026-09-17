# NordStack — Data Engineer Take-Home

An analytics layer over a fictional B2B SaaS billing system: MySQL as the operational source,
dlt for ingestion, PostgreSQL as the warehouse, dbt for modelling, Airflow for orchestration.

> **Status:** ingestion is built and verified. dbt models and the Airflow DAG are in progress.
> This README records the decisions taken so far and grows as the project does.

---

## Architecture

```
seed_data/*.csv
      │  bootstrap (once)
      ▼
┌──────────────┐   dlt    ┌──────────────┐   dbt    ┌─────────────────────────┐
│    MySQL     │ ───────► │  PostgreSQL  │ ───────► │ staging → intermediate  │
│   billing    │  replace │     raw      │          │  quarantine → marts     │
│  (source)    │          │ (mirror)     │          │  MRR · LTV · churn      │
└──────────────┘          └──────────────┘          └─────────────────────────┘
                                   ▲
                                   └──── Airflow orchestrates, every 5 min
```

Connection details for every service: **[CONNECTION_DETAILS.md](CONNECTION_DETAILS.md)**.
Full source-data analysis: **[DISCOVERY.md](DISCOVERY.md)**.
Engineering rules this repo follows: **[CLAUDE.md](CLAUDE.md)**.

---

## Quick start

```bash
cp .env.example .env
docker compose up -d                                  # MySQL + Postgres, wait for "healthy"

python -m venv venv && source venv/bin/activate        # Python 3.11
pip install -r requirements.txt

python ingestion/bootstrap_mysql.py                    # CSVs  → MySQL source
python ingestion/sync_mysql_to_postgres.py             # MySQL → Postgres raw
```

Both ingestion scripts are safe to re-run: counts stay at 121 / 175 / 2855 and never accumulate.

---

## Decisions and why

### 1. Two databases instead of `dbt seed`

The brief's optional Step 0 asks for a MySQL source synced with dlt. We took it because loading
CSVs straight into Postgres skips the part of the job that actually carries risk: crossing a
**source-to-warehouse boundary** repeatedly, on a schedule, without corrupting the destination.
Seeding proves nothing about idempotency, retries, or source fidelity.

MySQL therefore plays the operational billing system, and `seed_data/*.csv` is treated as an
extract *from* that system rather than as the source of truth.

### 2. `replace` instead of `merge` — the central ingestion decision

The roadmap suggested `write_disposition="merge"` on the business key, which is the usual answer
for idempotent ingestion. **We deliberately did not use it.**

dlt's merge runs a deduplication step — `ROW_NUMBER() OVER (PARTITION BY primary_key)` inside
`SqlMergeFollowupJob` (merge's default strategy is in fact `delete-insert`). The source contains
two *deliberately planted* duplicate rows, `C0023` and `S00006`. Under merge they would be
silently collapsed during ingestion, and `raw` would land 120/174 instead of 121/175.

The brief is explicit (§3): *"The raw data contains deliberately planted quality issues. Your
tests on the raw/staging layer should **catch** them."* A defect that no longer exists cannot be
caught. Merge would have quietly fixed the data and hidden a problem the assessment asks us to
surface.

`replace` gives us everything we need at once:

| Requirement | How `replace` satisfies it |
|---|---|
| Idempotent reruns | reloads rather than appends — counts never accumulate |
| Safe on Airflow retry | a retried run produces the identical table |
| Source fidelity | `raw` mirrors MySQL exactly, defects included |
| Defects testable by dbt | duplicates reach staging, where `unique` catches them |

The business key is **still declared** as a dlt resource hint. Verified against dlt 1.30: the
`primary_key` hint sets `primary_key: True` and `nullable: False`, but dlt maps only the `unique`
hint to a Postgres constraint (`HINT_TO_POSTGRES_ATTR = {"unique": "UNIQUE"}`), so no constraint
is emitted and the duplicates load intact. The key is documented in the schema without being
enforced where enforcement would destroy evidence.

**Cost:** a full reload each run instead of an incremental merge. At 3,151 rows that is free.
See *Next steps* for when this should be revisited.

**Consequence worth stating plainly:** deduplication is not skipped, it is *relocated* — from an
invisible side effect of ingestion to an explicit, tested step in dbt staging.

### 3. No constraints in the MySQL DDL

`ingestion/ddl/mysql_source.sql` declares no `PRIMARY KEY`, `FOREIGN KEY`, `CHECK`, or `NOT NULL`
on the affected columns. A real billing system would have all of them; each is omitted here
because it would reject a planted defect at `INSERT` time:

| Omitted | Would reject |
|---|---|
| `PRIMARY KEY` | duplicate rows `C0023`, `S00006` |
| `FOREIGN KEY` | orphans `S00011`→`C9999`, `I000601`→`S99999` |
| `CHECK` | negative money `S00048`, `I000725`–`I000731` |
| `NOT NULL` | blank country `C0008`, null amount `I000322` |

Every omission is commented in the DDL with the defect it protects, so it reads as intent rather
than oversight. Enforcement lives in the trust layer (dbt tests), not in the raw source.

### 4. No custom `loaded_at` / `updated_at` columns

dlt already stamps every row with `_dlt_load_id` and `_dlt_id`, and maintains a `_dlt_loads`
table containing `inserted_at` — so load lineage is available for free:

```sql
SELECT i.*, l.inserted_at AS loaded_at
FROM raw.invoices i
JOIN raw._dlt_loads l ON l.load_id = i._dlt_load_id;
```

An `updated_at` column was considered and rejected. The source is static and the DAG re-syncs
every 5 minutes, so the column would either never change (dead weight) or be set to `now()` on
every run — marking all 3,151 rows as modified every 5 minutes and destroying the very signal
the idempotency check depends on. Real change tracking is dlt's `merge` + `strategy="scd2"`
(`_dlt_valid_from` / `_dlt_valid_to`), which earns its place only once the source mutates.

### 5. Money as `DECIMAL`, never `FLOAT`

`monthly_price` and `amount` are `DECIMAL(10,2)` in MySQL and arrive as `numeric(10,2)` in
Postgres. Binary floats cannot represent 29.00 / 99.00 / 299.00 exactly, and these values sum
into every revenue figure in the marts. Verified: `SUM(amount)` is `382850.00` in the CSVs, in
MySQL, and in Postgres — no drift.

---

## Data-quality findings

14 defects and 3 edge cases, affecting 4 of 121 customers, 5 of 175 subscriptions, and 12 of
2,855 invoices. Summary below; full evidence, row IDs, and reasoning in
**[DISCOVERY.md](DISCOVERY.md)**.

| # | Issue | Example | Handling | Test that catches it |
|---|---|---|---|---|
| D1, D5 | Duplicate primary key (rows byte-identical) | `C0023`, `S00006` | deduplicate in staging — lossless, no conflicting values | `unique` + `not_null` |
| D2 | Blank `country` | `C0008` | normalize to `UNKNOWN`, keep row | `not_null` after normalization |
| D3 | Malformed `email` | `C0016` = `not-an-email` | flag, keep row — feeds no mart | warn-level singular test |
| D4 | `created_at` in the future, after its own subscription | `C0041` (2027-03-15) | flag, keep row | warn-level date test |
| D6, D10 | Orphan foreign key | `S00011`→`C9999`, `I000601`→`S99999` | **quarantine** | `relationships` |
| D7, D13 | Casing / trailing whitespace | `'ACTIVE'`, `'PAID '` | normalize `lower(trim(...))` **before** filtering | `accepted_values` |
| D8 | `end_date` before `start_date`, still billing 16 months | `S00034` | null the date, exclude from **churn only** — keep its €3,887 of paid revenue | **singular test B** |
| D9, D12 | Negative money | `S00048`, `I000725`–`I000731` | **quarantine** | **singular test A** |
| D11 | `paid` invoice with null `amount` | `I000322` | **quarantine** — do not impute | **singular test A** |
| D14 | Non-EUR currency | `I000101`, `I000201` (SEK) | convert via documented static FX rate | `accepted_values` on `currency` |
| E15–E17 | Future-dated subscription, future cancellations, one missed billing month | `S00149`, `S00020`, `S00040` | keep — real behaviour, not defects | documented assumptions |

Three defects had no single defensible reading and were decided explicitly — `S00034`
(trust the billing history over the corrupt date), the SEK invoices (treat the currency as real
and convert), and `I000322` (quarantine rather than invent €99 of revenue). The reasoning for
each is in [DISCOVERY.md §5](DISCOVERY.md).

**Quarantine is a model, not a `WHERE` clause.** Rejected rows land in `invalid_*` models with
their rejection reason, so "what happened to the bad records?" is answerable with a query.

---

## Tests

### Ingestion (`ingestion/sync_mysql_to_postgres.py`)

Validation runs after every sync and fails the process — and therefore the Airflow task — if any
check fails.

| Check | Why it exists |
|---|---|
| `raise_on_failed_jobs()` | dlt records failed jobs but still exits 0. Without this, a partial load would be reported as success. |
| Destination table queryable | catches a load that silently produced nothing |
| **Row-count parity** source vs destination | nothing lost, nothing double-loaded → 121 / 175 / 2855 |
| **Distinct-key parity** source vs destination | nothing silently deduplicated → 120 / 174 / 2855 |
| Primary key not null | the declared business key is actually populated |

Row-count parity alone is not enough: it would still pass if rows were deduplicated *and*
duplicated in equal measure. The distinct-key check is what would catch a regression back to
`merge` — it is the test that protects decision #2.

### Bootstrap (`ingestion/bootstrap_mysql.py`)

Fails before touching the database if a CSV or the DDL is missing; asserts exact row counts
after loading; rolls back and exits non-zero on any error.

### dbt (Phase 5 — in progress)

Generic tests (`unique`, `not_null`, `relationships`, `accepted_values`) plus two singular tests
encoding billing invariants: **(A)** no `paid` invoice has a null or non-positive amount, and
**(B)** `end_date` never precedes `start_date`. Raw-layer tests run at warn severity so planted
defects stay visible; staging and marts run at error severity so `dbt build` fails if bad data
reaches the trusted layer.

---

## Verified so far

| | Result |
|---|---|
| Bootstrap, run twice | 121 / 175 / 2855 both times |
| dlt sync, run twice | identical counts, `1 duplicate row preserved` on two tables |
| All 12 planted defects in `raw` | survived end-to-end, including `[PAID ]` with its trailing space |
| `SUM(amount)` across CSV → MySQL → Postgres | `382850.00`, no floating-point drift |
| Types preserved | `numeric(10,2)` and `date` carried through from MySQL |

---

## Next steps

Deliberately deferred — the current dataset does not justify them:

- **Incremental dbt models and `merge` ingestion**, once row volume makes full reloads
  expensive. At that point duplicate detection moves from dbt staging to the ingestion
  validation step.
- **SCD2 (`_dlt_valid_from` / `_dlt_valid_to`)**, once the source actually mutates.
- **CDC from MySQL**, replacing scheduled full extraction.
- **Dated historical FX rates**, replacing the static mapping.
- **Slim CI** (`state:modified+` with deferral), which needs a production manifest to defer to —
  none exists in a take-home with no deployment target.
- Postgres indexing and partitioning, a real staging environment, richer observability,
  production secrets management, RBAC, and infrastructure as code.
