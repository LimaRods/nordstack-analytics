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
          raw quality tests          data_issues
          severity: warn             issues_* models, built from raw
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

The `data_issues` models are built from raw, not from staging. Both branches
read the source:

```
raw
 ├── rule breaches → data_issues (issues_customers, issues_subscriptions, issues_invoices)
 └── all rows      → staging → intermediate → marts
```

This is what lets staging normalize freely. Detection runs against the source, so the
audit trail does not depend on what staging did first. If detection read staging instead,
a cleaned `'PAID '` would already look valid and the defect would disappear.

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
uv run dbt deps
uv run dbt build                             # models + tests, dev target
uv run dbt docs generate
uv run dbt docs serve --port 8081            # 8080 is taken by Airflow
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
rule breach                → recorded    data_issues, built from raw
record that cannot be used → quarantined excluded_from_marts, never reaches a consumer
trusted contract violated  → error       mart tests, fail the build
```

Two words, deliberately different. A row is **flagged** when it breaks a rule: it is
recorded in `data_issues` and still flows to the marts. It is **quarantined**
when it also carries `excluded_from_marts`, which keeps it out of the marts entirely.
29 rows are flagged; 21 of those are quarantined.

**Raw tests warn.** Raw shows what arrived. Cleaning it so the tests pass would destroy
the evidence. Twelve warnings fire on every build.

**Detection records, it does not decide.** Each `issues_*` model has one boolean per
rule breached, not a single reason column, so a row that breaks three rules reports all
three. Separating detection from the decision is the point: `excluded_from_marts` is the
only thing that withholds a row, and it is a column you can query rather than a `WHERE`
buried in a model. `I000451` has bad casing but its €29 is real, so it is flagged and
kept.

**Marts fail the build.** A failure there means a quarantined row reached a consumer.

The build is green because unusable records are excluded upstream, not because tests were
weakened: 40 pass, 12 warn, 0 errors.

### What was found

| Issue | Example | Handling |
|---|---|---|
| Duplicate business key | `C0023`, `S00006` | deduplicated in staging, flagged; the rows are identical, so nothing is lost |
| Orphan foreign key | `S00011`→`C9999`, `I000601`→`S99999` | quarantined |
| Negative money | `S00048` (−99.00) and its 7 invoices | quarantined |
| Paid invoice with null amount | `I000322` | quarantined, not imputed |
| `end_date` before `start_date` | `S00034` | flagged; excluded from churn only |
| Cancelled with a future `end_date` | `S00020`, `S00139`, `S00167` (2027) | not a defect: kept, marked `is_future_cancellation`. In a real project I would confirm the treatment with a domain expert |
| Casing and trailing whitespace | `'ACTIVE'`, `'PAID '` | normalized in staging, flagged |
| Blank country | `C0008` | labelled `UNKNOWN` in staging, flagged |
| Malformed email | `C0016` | flagged only; the `email` column doesn't feed any mart |
| Future `created_at` | `C0041` (2027) | flagged |
| Non-EUR currency | 2 SEK invoices | converted with a static rate |

`data_issues` holds 29 rows: 4 customers, 5 subscriptions and 20 invoices.
Of those, 21 are quarantined — 19 invoices and 2 subscriptions. Every flagged customer
still reaches the marts, because no customer defect is severe enough to withhold real
revenue.

Three cases had no obvious answer, so I decided them explicitly:

- **`S00034`** — the `end_date` is before the `start_date`, but the subscription kept
  billing for 16 months after it (17 invoices, €3,887 paid). The billing history is more
  reliable than the date, so the revenue stays in MRR and LTV and only the churn month is
  dropped. Churn covers 49 of 52 cancellations.
- **SEK invoices** — treated as a real currency and converted, not as EUR with a wrong
  label.
- **`I000322`** — quarantined, not imputed. Inventing €99 of revenue to keep a row is
  worse than losing the row.

This gives traceability today: "what happened to that row?" is a query, not a guess. In
a bigger system the same models are where remediation, alerts to the upstream owner and
reprocessing of corrected records would attach. None of that is implemented here.

---

## Modelling

**Staging** renames, casts, normalizes, removes duplicate business keys and applies
light repairs. A blank country becomes `'UNKNOWN'` here, so every consumer sees the gap
the same way instead of writing its own `COALESCE`.

The line is drawn at labels. Repairing a dimension is safe: the value is a label, and
`C0008` is still flagged because detection reads raw. Repairing a number is not, because
it changes a figure rather than how one is displayed. So a negative price stays negative,
a null amount stays null, and both are quarantined instead.

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
staging, data_issues, intermediate → views
marts                              → tables
```

### Lineage

`dbt docs generate` produces the full graph. Both branches out of raw are visible in it:
`issues_*` reads the sources directly, while the trusted path runs through staging and
intermediate into the three marts.

![dbt lineage graph](docs/dbt_lineage.png)

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

`dev` is the default and the only fallback, so `prod` has to be chosen deliberately. The
target is logged, pushed to XCom and shown in the email. `profiles.yml` reads every value
through `env_var()` and holds no literal credentials.

**This protects the dbt layer and nothing else.** `dlt_sync` writes `analytics.raw` on
every run whatever target is selected — the dataset name is fixed in the ingestion script,
and `validate_raw` reads the same schema. So a local `airflow dags test` cannot overwrite
a mart, but it does reload the raw tables a prod build reads.

That is tolerable here only because the source is static and `replace` is idempotent: the
reload lands the same 121 / 175 / 2,855 rows either way. It stops being tolerable the
moment raw has consumers, which is the argument for a separate dev analytics database in
Next steps. Isolating ingestion by environment is a deployment concern, not something one
more flag in the DAG should paper over.

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
| Issue detection | built from raw | catch defects before staging normalizes them away |
| Staging | format, standardization, light repairs | labels may be repaired, numbers may not |
| Mart tests | error | the published contract has to hold |
| Deduplication | dbt staging | visible and tested, not a side effect of ingestion |
| FX | static mapping | allowed by the brief; dated rates are a production concern |
| Default dbt target | `dev` | production has to be chosen, never inherited |
| CI selection | `state:modified+` with baseline | shows the pattern at almost no cost |

---

## Next steps

### What I would build next

**Separate the dbt repository.** dbt, ingestion and Airflow share one repository here. I
would move the dbt project into its own repository and pull it in as a submodule, so one
dbt project can serve the whole data department instead of being tied to this pipeline.

**End-to-end CI/CD.** Today CI/CD only covers dbt. It should cover the Airflow layer as
well: DAG imports, DAG ID, schedule, task dependencies, and the ingestion scripts run
against throwaway services. A change in any layer should be validated the same way, and
deployed the same way after merge.

**Semantic layer.** The metric definitions live inside the mart SQL. A semantic layer
would define MRR, LTV and churn once and let BI tools query those definitions, so the
same metric is not re-implemented in every dashboard.

**FX.** Replace the static rates with a dated rate table (`date | currency | eur_rate`)
loaded by its own pipeline, and convert each invoice at the rate for its accounting date.
Historical invoices should not be revalued at today's rate unless the business asks for
that.

### Production mindset: decisions waiting on a trigger

The three below are deliberately *not* implemented, and building them here would be cost
with no benefit at 3,151 rows. They are worth stating anyway, because the useful skill is
not knowing the pattern — it is knowing the condition that makes it correct, and being
able to say why that condition has not been met yet. Adding them without the trigger is
how a small pipeline becomes expensive to run and hard to change.

**Ingestion strategy.** `replace` is right here because the duplicates are planted on
purpose and have to survive ingestion. A real billing system would have a genuine unique
ID per table, enforced at the source. With reliable keys, `merge` on the business key is
the natural choice, and CDC after that, instead of extracting everything on a schedule.

> Trigger: enforced keys at the source, plus a full reload that costs real time or money.

**Materializations.** Tables for large reused intermediate models, incremental for large
marts that are rebuilt often. Incremental brings its own questions — `unique_key`,
late-arriving data, lookback windows, full refresh — and each one is a decision that can
silently produce wrong numbers if answered badly. None of them are worth answering while
a full build takes five seconds.

> Trigger: model runtime or warehouse cost measured, not assumed.

**A dev analytics database.** Here `dev` and `prod` are two schemas in one database, and
both read the same `raw`:

```
analytics
 ├── raw      one ingestion run, shared by both targets
 ├── dev      dbt --target dev
 └── prod     dbt --target prod
```

That is fine for one person on a static dataset, and wrong as soon as it is not. `raw` is
the shared part that matters: testing an ingestion change means writing into the same
tables production reads, and there is no way to try a schema change without touching them.

With a real platform the environments separate at the database, each with its own
ingestion:

```
  PRODUCTION                                DEVELOPMENT

  MySQL, live billing                       MySQL read replica or
        │                                   sanitized snapshot
  Airflow, prod deployment                        │
        │  dlt_sync                          Airflow, local or dev
        ▼                                         │  dlt_sync
  analytics_prod.raw ───── clone or subset ─────► analytics_dev.raw
        │                  one direction only           │
        │  dbt --target prod, CD only                   │  dbt --target dev
        ▼                                               ▼
  analytics_prod.marts                        analytics_dev.dbt_<developer>
        │                                     one schema per person, safe to drop
        ▼
  BI, reverse ETL, consumers

  CI, per pull request
  ephemeral database created and dropped with the job, deferring to the manifest
  CD published
```

The two routes into `analytics_dev.raw` are alternatives, not both at once:

| Changing | Route |
|---|---|
| dbt models | clone prod raw down; never touch the source |
| ingestion code | run `dlt_sync` against the replica or fixtures |

The source system is the same in both columns — what differs is which copy development is
allowed to read. Extracting from the live primary every few minutes is load the billing
system did not ask for, and it hands developers production credentials they rarely need.

The same DAG file runs in both columns. Nothing in it names an environment: the deployment
supplies the connections, so `dlt_sync` lands in whichever `raw` its Airflow points at, and
dbt follows with the matching target.

Three properties that the current setup cannot offer: ingestion changes are tried against
`analytics_dev.raw` and cannot reach production, two people can build at once without
overwriting each other, and production is written by CD alone rather than by whoever has
the credentials.

The arrow only ever points down. Production data may be cloned into development; nothing
in development writes upward.

> Trigger: more than one person developing, or a production raw layer that consumers
> depend on.

---

## Closing

The dataset is small, so the implementation is simple. Simple does not have to mean
careless. Raw keeps its defects visible, invalid records are isolated before they reach
the trusted path, marts enforce contracts that fail the build, retries are bounded, dev
and prod are separated, and CI validates changes before merge.

Those boundaries are the part that scales. Ingestion, materializations and CI/CD can each
change later without disturbing the rest.
