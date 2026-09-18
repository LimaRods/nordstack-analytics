# NordStack — Data Engineer Take-Home

An analytics layer over a fictional B2B SaaS billing system: MySQL as the operational source,
dlt for ingestion, PostgreSQL as the warehouse, dbt for modelling, Airflow for orchestration.

> **Status:** complete. Ingestion, the dbt project (staging, quarantine, intermediate, marts),
> the dbt CI/CD workflows and the Airflow DAG are all built and verified.
> Repository: <https://github.com/LimaRods/nordstack-analytics>

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

### 5. Quarantine reads the source, so staging is free to standardize

The layers went through two revisions before settling, and the final shape is the point:

| Layer | Role |
|---|---|
| `raw` | faithful mirror, defects included |
| `quarantine` | detects defects **from the source**, independent of staging |
| `staging` | standardizes format, guarantees grain, repairs nothing |
| `marts` | repairs, filters, decides |

The first attempt had quarantine read *from staging*, which forced a false choice: normalizing
`'PAID '` to `'paid'` made the defect undetectable, but leaving it raw meant every downstream
filter had to remember `lower(trim(status))` or silently drop €29 of revenue.

Pointing quarantine at the source dissolves it. The same invoice now reads `paid` in staging and
`PAID ` in quarantine, simultaneously — usable *and* auditable. Normalizing and preserving
stopped being mutually exclusive.

The line drawn: **standardize format, never substitute values.** Casing, whitespace and types are
reversible and lose no information, so they belong in staging. Inventing a value the source never
had — a blank country becoming `UNKNOWN`, nulling `S00034`'s impossible date — is a business
decision, so it belongs in the mart that needs it.

One consequence: rejection reasons are **one boolean per rule**, not a single label. A row can
breach several rules at once — `I000725`–`I000731` are both non-positive *and* children of a
quarantined subscription — and a single `invalid_reason` string reported the first match and
silently dropped the rest.

### 6. Money as `DECIMAL`, never `FLOAT`

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
| D1, D5 | Duplicate primary key (rows byte-identical) | `C0023`, `S00006` | deduplicate in staging — a duplicate is a *grain* problem, not formatting, and the rows are identical so it is lossless | `unique` on the source |
| D2 | Blank `country` | `C0008` | staging leaves it `NULL`; the LTV mart `COALESCE`s it to `UNKNOWN` | singular test |
| D3 | Malformed `email` | `C0016` = `not-an-email` | flag only — feeds no mart | singular test |
| D4 | `created_at` in the future, after its own subscription | `C0041` (2027-03-15) | flag only | singular test |
| D6, D10 | Orphan foreign key | `S00011`→`C9999`, `I000601`→`S99999` | **quarantine**, excluded from marts | `relationships` |
| D7, D13 | Casing / trailing whitespace | `'ACTIVE'`, `'PAID '` | standardized in staging, so no consumer has to remember `lower(trim(...))` | `accepted_values` |
| D8 | `end_date` before `start_date`, still billing 16 months | `S00034` | null the date, exclude from **churn only** — keep its €3,887 of paid revenue | **singular test B** |
| D9, D12 | Negative money | `S00048`, `I000725`–`I000731` | **quarantine** | **singular test A** |
| D11 | `paid` invoice with null `amount` | `I000322` | **quarantine** — do not impute | **singular test A** |
| D14 | Non-EUR currency | `I000101`, `I000201` (SEK) | convert via documented static FX rate | `accepted_values` on `currency` |
| E15–E17 | Future-dated subscription, future cancellations, one missed billing month | `S00149`, `S00020`, `S00040` | keep — real behaviour, not defects | documented assumptions |

Three defects had no single defensible reading and were decided explicitly — `S00034`
(trust the billing history over the corrupt date), the SEK invoices (treat the currency as real
and convert), and `I000322` (quarantine rather than invent €99 of revenue). The reasoning for
each is in [DISCOVERY.md §5](DISCOVERY.md).

**Quarantine is a model, not a `WHERE` clause.** Every defect lands in an `invalid_*` model with
**one boolean per rule breached**, so "what happened to the bad records?" is answerable with a
query — and a row breaching three rules reports all three. `excluded_from_marts` separates rows
withheld from revenue from rows merely repaired: `I000451`'s casing was fixed, but its €29 is
real and still counts.

All 13 defects are traceable there. Verified: 4 customers, 5 subscriptions, 20 invoices, of
which 19 are excluded from revenue.

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

### dbt

Severity encodes **where the contract applies**:

| Layer | Severity | Why |
|---|---|---|
| raw sources | **warn** | the source is expected to be dirty. These tests exist to make the planted defects visible in every build, never to block it |
| staging | *none* | it standardizes format and asserts nothing. Testing it would restate the raw tests against a copy of the same rows |
| marts | **error** | the published contract. A failure means bad data escaped quarantine |

20 tests on raw, of which **12 fire on every build** — one per planted defect, except singular
test A which catches two at once (D11 + D12, `WARN 6`).

Five singular tests run against raw at warn severity: paid-amount validity, date chronology,
positive price, email format, and non-future signup. Three run against the marts at error
severity:

- **`assert_no_quarantined_invoice_in_revenue`** — joins the revenue base against quarantine.
  While it passes, `excluded_from_marts` is genuinely *enforced* rather than merely computed.
- **`assert_mart_revenue_reconciles_to_source`** — `fct_mrr` and `customer_ltv` aggregate the
  same base along different axes, so their totals must agree. A divergence means a join fanned
  out — a bug neither mart would reveal alone.
- **`assert_mart_amounts_are_non_negative`** — nothing published may be negative.

The build is green *because defects are quarantined upstream*, not because tests were weakened.
Running the same rules without the accepted-set filter returns exactly the 6 / 1 / 1 rows
DISCOVERY.md predicted.

---

## CI/CD

Two workflows, dbt only. Both read every connection setting from repository variables and
secrets — see [CONNECTION_DETAILS.md](CONNECTION_DETAILS.md).

| Workflow | Trigger | Builds | Target |
|---|---|---|---|
| [`dbt-ci.yml`](.github/workflows/dbt-ci.yml) | pull request | `state:modified+` | `dev` |
| [`dbt-cd.yml`](.github/workflows/dbt-cd.yml) | merge to `main`, or manual | everything | `prod` |

**CI is component-aware twice over.** It only runs when `dbt/`, `ingestion/` or `seed_data/`
change — an Airflow-only commit builds no warehouse. Within that, `state:modified+` selects the
changed models *plus everything downstream*: the `+` is what stops a break hiding behind an
untouched consumer. Verified on a real PR — editing `stg_invoices` rebuilt
`int_paid_invoices_eur` → `fct_mrr` + `customer_ltv` and their tests, while `subscription_churn`
and the quarantine models were correctly skipped.

**CD builds everything, deliberately.** A deployment must leave production internally consistent
and every test must pass against what is *published*, not only against what changed in that
commit. It publishes `manifest.json` as an artifact.

`DBT_TARGET` is set to `prod` in exactly one place: the CD workflow. CI leaves it unset, so
`profiles.yml` falls back to `dev` and a pull request can never write to production.

### The honest cost of Slim CI here

`--defer --state` needs two things: a manifest to diff against, and **real tables** for the
unmodified models to resolve to. A company has both — CI points at a warehouse that already
exists. A GitHub runner has neither: it is ephemeral, the service containers start empty, and
nothing survives the job.

So CI bootstraps MySQL from the CSVs, syncs to Postgres, *then* builds the base branch to have
something to defer to. Measured on a real run:

```
58s  start service containers     ← unavoidable
23s  install dependencies         ← unavoidable
 5s  dlt sync
 5s  build baseline from main     ← exists only because there is no persistent warehouse
 5s  build modified + downstream
```

**dbt is 10 seconds of a 1m51s run, and the full build is 5 seconds.** Selective building costs
more than it saves at this scale, and because CI rebuilds the whole warehouse from CSVs anyway,
the full cost is already paid before dbt starts.

Kept as-is deliberately: it demonstrates the pattern, and the substitution is documented rather
than hidden. With a persistent warehouse the baseline step simply disappears and `--state` points
at the `prod-manifest` artifact CD already publishes — a one-step change, not a redesign. Slim CI
pays for itself when a full build takes 40 minutes; here it takes 5 seconds.

---

## Orchestration

One DAG, [`nordstack_analytics`](airflow/dags/nordstack_analytics.py), on the five-minute
schedule the brief asks for, emailing on both success and failure.

```
dlt_sync  ->  validate_raw  ->  dbt_build  ->  email (success or failure)
```

**Three tasks rather than one script, deliberately.** A failure names itself: a red `dlt_sync`
is an ingestion problem, a red `validate_raw` means ingestion *reported* success while the
warehouse disagrees, and a red `dbt_build` is a modelling or data-quality problem. Airflow also
retries only the task that failed instead of redoing work that already succeeded.

| Setting | Value | Why |
|---|---|---|
| `schedule` | `*/5 * * * *` | required by the brief |
| `catchup` | `False` | nothing is gained by backfilling five-minute intervals |
| `max_active_runs` | `1` | two runs replacing the same tables would race each other |
| `retries` | `2`, 30s apart | recovers a dropped connection; a broken model fails identically every time and should surface, not spin |
| `execution_timeout` | 4 min/task | a task outliving the interval is stuck, not slow |
| `dagrun_timeout` | 4 min | under the interval, so a stuck run cannot pile up |
| `sla` | 2/3/4 min per task | lateness is the first symptom of runs about to overlap |

**Retries are safe because every task is idempotent.** dlt uses `replace` (reloads, never
appends), `validate_raw` only reads, and dbt rebuilds in full. That is what makes retries a
recovery mechanism rather than a duplication risk.

**Notifications are separated by meaning.** Success and failure emails fire on the DAG run —
failure *after retries are exhausted*, not on every attempt, since an alert that fires on
recoverable errors trains people to ignore it. SLA misses get their own callback: a late run is
not a broken one. Sending goes through `smtplib` with credentials from the environment, so the
whole notification path is in version control rather than in `airflow.cfg`.

**The trap worth naming:** `PythonOperator` marks a task successful unless the callable
*raises*. Both ingestion scripts are CLIs whose `main()` **returns** an exit code, so calling
them directly would report a failed load as a green task. The `_run()` adapter converts the exit
code into an exception. Verified by pointing `MYSQL_HOST` at a dead host and confirming the task
goes red.

**`validate_raw` repeats a check the sync script already does**, on purpose. A loader that
reports success while leaving the warehouse wrong is exactly the failure a self-check cannot
catch. Verified by deleting a row from `raw.customers` and confirming the task fails with
`121 rows at source, 120 in raw`.

---

## Verified so far

| | Result |
|---|---|
| Bootstrap, run twice | 121 / 175 / 2855 both times |
| dlt sync, run twice | identical counts, `1 duplicate row preserved` on two tables |
| All 13 planted defects | survived into `raw`, and every one is traceable in quarantine |
| `fct_mrr` vs `customer_ltv` | both €322,890.01 — reconciles to the source to the cent |
| Churn coverage | 49 of 52 cancellations; the 3 excluded are documented |
| CI on a real PR | built only the invoice lineage, skipped `subscription_churn` |
| CD on `main` | full build against `prod`, manifest and docs published |
| Airflow DAG, end to end | `state=success` in 6.4s, success email delivered |
| DAG failure paths | dead MySQL host and a corrupted `raw` both turn the task red |
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
- **Slim CI without the baseline build.** `state:modified+` is implemented, but CI has to build
  the base branch first because the runner has no persistent warehouse to defer to. With one,
  that step disappears and `--state` points at the `prod-manifest` CD already publishes. Worth
  doing when a full build stops taking 5 seconds.
- **CI/CD for Airflow, and end-to-end rather than dbt-only.** Today only the dbt project is
  covered. With more time: validate that every DAG imports, assert the DAG id, schedule,
  `catchup=False` and task dependencies in a unit test, and run the ingestion scripts against
  throwaway services on every PR — then promote the DAGs to the scheduler on merge the way
  `dbt-cd.yml` promotes the models.
- **A JSONB array of breached rules** in quarantine, replacing one boolean column per rule.
  Booleans compose correctly, which was the important fix; the array is about not adding a
  column every time a rule is added.
- Postgres indexing and partitioning, a real staging environment, richer observability,
  production secrets management, RBAC, and infrastructure as code.
