# NordStack — Data Engineer Take-Home

An analytics layer over a fictional B2B SaaS billing system.

```
seed_data/*.csv  →  MySQL  →  dlt  →  PostgreSQL raw  →  dbt  →  marts
```

The CSVs initialize a simulated billing source in MySQL. dlt syncs MySQL into the
PostgreSQL `raw` schema. dbt builds three marts from it: MRR, customer LTV and churn.
Airflow runs the pipeline on a schedule. GitHub Actions validates changes before merge.

Building a pipeline that produces numbers is not the hard part. The hard part is making
it safe to run twice, making failures visible, keeping environments apart, and enforcing
data quality where it belongs. Most of the decisions below are about that.

The dataset is small: 3,151 rows in three tables. The implementation stays simple
because of that, and each section separates what is built from what would change at real
volume.

---

## Architecture

```
                        seed_data/*.csv
                              │
                              ▼
                            MySQL                    simulated billing source
                              │
                             dlt                     replace, business key declared
                              │
                              ▼
                       PostgreSQL raw                mirrors the source, defects included
                              │
                  ┌───────────┴───────────┐
                  │                       │
                  ▼                       ▼
          raw quality tests          quarantine
          severity: warn             invalid_* models, built from raw
                  │
                  ▼
               staging                                rename, cast, normalize
                  │
                  ▼
            intermediate                              reusable business logic
                  │
                  ▼
                marts                                 error-severity tests
                  │
          ┌───────┼───────┐
          ▼       ▼       ▼
         MRR     LTV    Churn
```

Quarantine is built from raw, not from staging. Both branches read the source:

```
raw
 ├── invalid rows → quarantine (invalid_customers, invalid_subscriptions, invalid_invoices)
 └── accepted rows → staging → intermediate → marts
```

This is what lets staging normalize freely. Detection runs against the source, so the
audit trail does not depend on what staging did first. If quarantine read staging
instead, a cleaned `'PAID '` would already look valid and the defect would disappear.

---

## Quick start

```bash
cp .env.example .env
docker compose up -d                        # MySQL + Postgres; wait for "healthy"

python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python ingestion/bootstrap_mysql.py          # CSVs  → MySQL
python ingestion/sync_mysql_to_postgres.py   # MySQL → Postgres raw

cd dbt
dbt deps
dbt build                                    # models + tests, dev target
dbt docs generate && dbt docs serve
```

Both ingestion scripts are safe to re-run. Counts stay at 121 / 175 / 2,855.

Airflow:

```bash
export AIRFLOW_HOME="$(pwd)/airflow"
airflow standalone                           # UI on :8080
airflow dags test nordstack_analytics        # one full run, no scheduler
```

Connection details: [CONNECTION_DETAILS.md](CONNECTION_DETAILS.md).

---

## Why MySQL → dlt → PostgreSQL

The brief allows loading the CSVs straight into Postgres. I took the optional MySQL path
because seeding skips the part that carries risk: crossing a source-to-warehouse
boundary repeatedly, on a schedule, without corrupting the destination. `dbt seed` says
nothing about idempotency, retries or source fidelity.

MySQL plays the operational billing system. Postgres is the analytical destination.

### `replace`, not `merge`

dlt's merge deduplicates on the primary key before writing. The source has two planted
duplicates, `C0023` and `S00006`, so merge would collapse them during ingestion and load
120/174 rows instead of 121/175. That fixes the data quietly and removes the defect the
tests are supposed to catch.

`replace` reloads instead of appending, so repeated runs and Airflow retries end at the
same row counts. Deduplication moves to dbt staging, where it is visible and tested.

The business key is still declared as a dlt resource hint. dlt only turns the `unique`
hint into a Postgres constraint, so `primary_key` documents the key without rejecting the
duplicates.

This is the right choice for this dataset, not a general rule. See Next steps.

---

## Data quality

The source ships with planted defects. The question is not how to clean them. It is
where each type of problem belongs.

```
source defect              → warn        raw tests, visible in every build, never block it
known-invalid record       → quarantine  isolated from raw, before staging
trusted contract violated  → error       mart tests, fail the build
```

**Raw tests warn.** Raw shows what arrived. Cleaning it so the tests pass would destroy
the evidence. Twelve warnings fire on every build.

**Quarantine isolates.** Each `invalid_*` model has one boolean per rule breached, not a
single reason column, so a row that breaks three rules reports all three.
`excluded_from_marts` separates rows kept out of revenue from rows that are only untidy.
`I000451` has bad casing, but its €29 is real and still counts.

**Marts fail the build.** A failure there means bad data got past quarantine and reached
a consumer.

The build is green because defects are quarantined upstream, not because tests were
weakened: 40 pass, 12 warn, 0 errors.

### What was found

| Issue | Example | Handling |
|---|---|---|
| Duplicate business key | `C0023`, `S00006` | deduplicated in staging; the rows are identical, so nothing is lost |
| Orphan foreign key | `S00011`→`C9999`, `I000601`→`S99999` | quarantined, excluded from marts |
| Negative money | `S00048` (−99.00) and its 7 invoices | quarantined, excluded |
| Paid invoice with null amount | `I000322` | quarantined, not imputed |
| `end_date` before `start_date` | `S00034` | kept and flagged, excluded from churn only |
| Casing and trailing whitespace | `'ACTIVE'`, `'PAID '` | normalized in staging |
| Blank country | `C0008` | stays NULL in staging, reported as `UNKNOWN` in the LTV mart |
| Malformed email | `C0016` | flagged only, feeds no mart |
| Future `created_at` | `C0041` (2027) | flagged only |
| Non-EUR currency | 2 SEK invoices | converted with a static rate |

Quarantine holds 4 customers, 5 subscriptions and 20 invoices. 19 invoices and 2
subscriptions are kept out of the marts.

Three cases had no obvious answer, so I decided them explicitly:

- **`S00034`** — the `end_date` is before the `start_date`, but the subscription kept
  billing for 16 months after it (17 invoices, €3,887 paid). The billing history is more
  reliable than the date, so the revenue stays in MRR and LTV and only the churn month is
  dropped. Churn covers 49 of 52 cancellations.
- **SEK invoices** — treated as a real currency and converted, not as EUR with a wrong
  label.
- **`I000322`** — quarantined, not imputed. Inventing €99 of revenue to keep a row is
  worse than losing the row.

Quarantine gives isolation and traceability today. In a bigger system it is also where
remediation and alerts to the upstream owner would attach. That is not implemented here.

---

## Modelling

**Staging** renames, casts, normalizes and removes duplicate business keys. It
standardizes format but does not repair values. A blank country stays blank, a negative
price stays negative. Replacing a value the source never had is a business decision, so
it belongs in the mart that needs it.

**Intermediate** holds logic used by more than one mart: the EUR revenue base, the
mart-eligible subscriptions and the FX reference. Logic used by a single mart stays in
that mart.

**Marts** answer the three questions in the brief:

| Model | Grain | Definition |
|---|---|---|
| `fct_mrr` | month × plan | sum of paid invoice amounts in EUR, by invoice month |
| `customer_ltv` | customer | total paid revenue, with country, plan mix and current status |
| `subscription_churn` | cancellation month | cancellations and MRR lost, from `monthly_price` |

MRR is recognised revenue, not contracted value. The brief asks for MRR from paid
invoices, so open and failed invoices count for nothing. For a subscription that pays on
time both definitions give the same number. They differ exactly where payment failed,
which is the reason to use the paid basis.

`customer_ltv` uses LEFT JOINs, so a customer whose subscription has not billed yet still
appears with zero revenue instead of disappearing.

`subscription_churn` reports scheduled future cancellations with an
`is_future_cancellation` flag instead of filtering them. They are real commitments, but a
consumer should not read them as churn that already happened.

Both marts total €322,890.01, and a mart test checks this on every build.

Materializations follow current volume, not a rule:

```
staging, quarantine, intermediate → views
marts                             → tables
```

---

## FX assumption

Reporting currency is EUR. `int_fx_rates` holds a static mapping (`SEK → 0.087`), which
the brief allows. Every invoice converts at the same rate whatever its date. This affects
one paid invoice, worth €26 out of €322,890.

The model also works as a guard. `int_paid_invoices_eur` joins it with an INNER join, so
a currency with no rate cannot reach revenue at face value. It drops out, and the
reconciliation test fails.

---

## Orchestration

One DAG, [`nordstack_analytics`](airflow/dags/nordstack_analytics.py), on the five-minute
schedule the brief asks for, with email on success and failure.

```
dlt_sync  →  validate_raw  →  dbt_build  →  email
```

Three tasks instead of one script, so a failure says what it is. A red `dlt_sync` is an
ingestion problem. A red `validate_raw` means ingestion reported success while the
warehouse disagrees. A red `dbt_build` is a modelling or data-quality problem. Airflow
also retries only the task that failed.

| Setting | Value | Reason |
|---|---|---|
| `retries` | 2, 30s apart | transient infrastructure errors only |
| `execution_timeout` | 4 min | a task that outlives the interval is stuck, not slow |
| `dagrun_timeout` | 4 min | under the schedule, so runs cannot pile up |
| `catchup` | `False` | backfilling five-minute intervals gains nothing |
| `max_active_runs` | 1 | two runs replacing the same tables would race |
| `sla` | 2 / 3 / 4 min | lateness is the first sign of overlap |

Retries are for transient failures. Broken SQL, a schema change or a failed data-quality
test fails the same way every attempt, so it should stay visible instead of retrying.

Two problems this caught. `PythonOperator` only marks a task failed if the callable
raises, and both ingestion scripts are CLIs whose `main()` returns an exit code, so
calling them directly reported a failed load as green. An adapter turns the exit code
into an exception. And `validate_raw` repeats a check the sync script already does, on
purpose, because a loader that reports success while leaving the warehouse wrong is
exactly what a self-check cannot catch. I verified both by breaking them: a dead
`MYSQL_HOST`, and a row deleted from `raw.customers`.

### Environment isolation

Only one database exists, so the targets separate by schema. `dbt_build` resolves its
target per run: the run's `dbt_target` param, then `DBT_TARGET`, then `dev`.

```
scheduled run / airflow dags test  →  dev
Trigger DAG w/ config → prod       →  prod
```

`dev` is the default and the only fallback, so local DAG testing cannot reach production.
`prod` has to be chosen. The target is logged, pushed to XCom and shown in the email.
`profiles.yml` reads every value through `env_var()` and holds no literal credentials.

---

## CI/CD

| Workflow | Trigger | Builds | Target |
|---|---|---|---|
| [`dbt-ci.yml`](.github/workflows/dbt-ci.yml) | pull request | `state:modified+` | `dev` |
| [`dbt-cd.yml`](.github/workflows/dbt-cd.yml) | merge to `main`, or manual | everything | `prod` |

CI runs only when `dbt/`, `ingestion/` or `seed_data/` change, so an Airflow-only commit
does not build the warehouse. `state:modified+` selects the changed models plus
everything downstream. The `+` matters: it stops a break from hiding behind a consumer
nobody touched. Verified on a real PR, where editing `stg_invoices` rebuilt
`int_paid_invoices_eur`, `fct_mrr` and `customer_ltv` and skipped `subscription_churn`.

CD builds everything. A deployment has to leave production consistent, and every test has
to pass against what is published, not only against what changed in that commit.
`DBT_TARGET=prod` is set in one place only, the CD workflow. CI leaves it unset, so a
pull request cannot write to production.

**What Slim CI costs here.** `--defer --state` needs a manifest to compare against and
real tables for the unmodified models. A GitHub runner has neither, so CI bootstraps
MySQL, syncs to Postgres and builds the base branch first to have something to defer to.
On a measured run the baseline takes 5s, the selective build 5s, and dbt is 10s out of
1m51s. At this size selective building costs more than it saves. I kept it because
replacing the baseline with a real production manifest is a one-line change.

This is CI, not CD in the deployment sense. There is no persistent platform to deploy to.

---

## Key decisions

| Decision | Choice | Reason |
|---|---|---|
| Source boundary | MySQL → dlt → Postgres | exercises real ingestion, not seeding |
| Write disposition | `replace` | small static dataset, keeps the planted defects |
| Raw tests | warn | detect source defects without blocking the build |
| Quarantine | built from raw | isolate invalid records before staging normalizes them |
| Staging | format only, no repair | replacing a value is a business decision |
| Mart tests | error | the published contract has to hold |
| Deduplication | dbt staging | visible and tested, not a side effect of ingestion |
| FX | static mapping | allowed by the brief; dated rates are a production concern |
| Default dbt target | `dev` | production has to be chosen, never inherited |
| CI selection | `state:modified+` with baseline | shows the pattern at almost no cost |

---

## Next steps

**Ingestion.** `replace` is right here because the duplicates are planted on purpose and
have to survive ingestion. A real billing system would have a real unique ID per table,
enforced at the source. With reliable keys, `merge` on the business key is the natural
choice, and CDC after that, instead of extracting everything on a schedule.

**Separate the dbt repository.** dbt, ingestion and Airflow share one repository here. I
would move the dbt project into its own repository and pull it in as a submodule, so one
dbt project can serve the whole data department instead of being tied to this pipeline.

**End-to-end CI/CD.** Today CI/CD only covers dbt. It should cover the Airflow layer as
well: DAG imports, DAG ID, schedule, task dependencies, and the ingestion scripts run
against throwaway services. A change in any layer should be validated the same way, and
deployed the same way after merge.

**A dev analytics database.** Here `dev` and `prod` are two schemas in the same database
and both read the same `raw`. In a real setup development would have its own analytics
database with its own raw layer, so ingestion work never writes into production raw
tables.

**Semantic layer.** The metric definitions live inside the mart SQL. A semantic layer
would define MRR, LTV and churn once and let BI tools query those definitions, so the
same metric is not re-implemented in every dashboard.

**FX.** Replace the static rates with a dated rate table (`date | currency | eur_rate`)
loaded by its own pipeline, and convert each invoice at the rate for its accounting date.
Historical invoices should not be revalued at today's rate unless the business asks for
that.

**Materializations.** Tables for large reused intermediate models, incremental for large
marts that are rebuilt often. Incremental brings its own questions (`unique_key`,
late-arriving data, lookback windows, full refresh), and none of them are worth answering
at 3k rows.

---

## Closing

The dataset is small, so the implementation is simple. Simple does not have to mean
careless. Raw keeps its defects visible, invalid records are isolated before they reach
the trusted path, marts enforce contracts that fail the build, retries are bounded, dev
and prod are separated, and CI validates changes before merge.

Those boundaries are the part that scales. Ingestion, materializations and CI/CD can each
change later without disturbing the rest.
