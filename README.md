# NordStack — Data Engineer Take-Home

An analytics layer over a fictional B2B SaaS billing system.

```
seed_data/*.csv  →  MySQL  →  dlt  →  PostgreSQL raw  →  dbt  →  marts
```

The CSVs initialize a simulated billing source in MySQL. dlt syncs MySQL into the
PostgreSQL `raw` schema. dbt turns raw billing data into three trusted marts — MRR,
customer LTV, and churn. Airflow runs the whole thing on a schedule; GitHub Actions
validates changes before merge.

Getting a pipeline to produce numbers is the easy half. The decisions worth discussing
are about the second run, the retry, the bad source record, and the change that looks
harmless — repeated execution has to be safe, failures visible and contained,
environments separate, and quality rules enforceable rather than aspirational.

This is a small environment: 3,151 rows across three tables. The implementation stays
simple on purpose, and the sections below separate what is built from how it would
evolve under real volume.

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

**Quarantine is built from raw, not from staging.** Both branches read the source
independently:

```
raw
 ├── invalid rows → quarantine (invalid_customers, invalid_subscriptions, invalid_invoices)
 └── accepted rows → staging → intermediate → marts
```

That ordering is what lets staging normalize freely. Detection happens against the
source, so the audit trail does not depend on what staging did first — and a normalized
`'PAID '` can't quietly clear a row of the checks it should have failed.

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

Both ingestion scripts are safe to re-run: counts stay at 121 / 175 / 2,855.

Airflow:

```bash
export AIRFLOW_HOME="$(pwd)/airflow"
airflow standalone                                   # UI on :8080
airflow dags test nordstack_analytics                # one full run, no scheduler
```

Connection details for every service: [CONNECTION_DETAILS.md](CONNECTION_DETAILS.md).

---

## Why MySQL → dlt → PostgreSQL

The brief allows seeding Postgres directly. I took the optional MySQL path because
seeding skips the part that carries risk — crossing a source-to-warehouse boundary
repeatedly, on a schedule, without corrupting the destination. `dbt seed` exercises
nothing about idempotency, retry safety or source fidelity.

MySQL plays the operational billing system; Postgres is the analytical destination.

### `replace`, not `merge`

dlt's merge deduplicates on the primary key before writing
(`ROW_NUMBER() OVER (PARTITION BY ...)`). The source contains two deliberately planted
duplicates, `C0023` and `S00006`. Merge would collapse them during ingestion and land
120/174 rows instead of 121/175 — quietly fixing data the assignment asks the tests to
catch.

`replace` reloads rather than appends, so repeated runs and Airflow retries converge on
the same row counts. Deduplication moves to dbt staging, where it is visible and tested.

The business key is still declared as a resource hint: dlt maps only the `unique` hint to
a Postgres constraint, so `primary_key` documents the key without emitting one that would
reject the duplicates.

This is right for a small static dataset, not a general rule:

| | Strategy |
|---|---|
| Small, static, defects must survive | `replace` — simple, deterministic |
| Large, changing, reliable business keys | `merge` / upsert, then CDC |

In a real transactional source I would expect keys and constraints to be part of the
data model rather than something the warehouse discovers.

---

## Data quality

The source ships with planted defects. The design question is not how to clean them —
it's where each kind of failure belongs.

```
source defect              → warn      (raw tests: visible in every build, never block it)
known-invalid record       → quarantine (isolated from raw, before staging)
trusted contract violated  → error     (mart tests: fail the build)
```

**Raw tests warn.** Raw shows what arrived; cleaning it so tests pass would destroy the
evidence. Twelve fire on every build.

**Quarantine isolates.** Each `invalid_*` model carries one boolean per rule breached
rather than a single reason label, so a row failing three rules reports all three.
`excluded_from_marts` separates rows withheld from revenue from rows that are merely
untidy — `I000451`'s casing is wrong, but its €29 is real and still counts.

**Marts fail.** There, a failure means bad data escaped quarantine and reached a
consumer.

The build is green because defects are quarantined upstream, not because tests were
weakened: 40 pass, 12 warn, 0 errors.

### What was found

| Issue | Example | Handling |
|---|---|---|
| Duplicate business key | `C0023`, `S00006` | deduplicated in staging — rows are byte-identical, so it's lossless |
| Orphan foreign key | `S00011`→`C9999`, `I000601`→`S99999` | quarantined, excluded from marts |
| Negative money | `S00048` (−99.00) and its 7 invoices | quarantined, excluded |
| Paid invoice, null amount | `I000322` | quarantined — not imputed |
| `end_date` before `start_date` | `S00034` | kept, flagged; excluded from **churn only** |
| Casing / trailing whitespace | `'ACTIVE'`, `'PAID '` | normalized in staging |
| Blank country | `C0008` | stays NULL in staging; LTV mart reports `UNKNOWN` |
| Malformed email | `C0016` | flagged only — feeds no mart |
| Future `created_at` | `C0041` (2027) | flagged only |
| Non-EUR currency | 2 SEK invoices | converted at a static rate (below) |

Quarantine holds 4 customers, 5 subscriptions and 20 invoices; 19 invoices and 2
subscriptions are withheld from the marts.

Three cases had no obvious right answer and were decided explicitly:

- **`S00034`** — its `end_date` precedes its `start_date`, yet it kept billing for 16
  months afterwards (17 invoices, €3,887 paid). The billing history is more credible
  than the date, so its revenue stays in MRR and LTV and only its churn month is
  discarded. Cost: 49 of 52 cancellations are counted, documented rather than absorbed.
- **The SEK invoices** — treated as a real currency and converted, not as mislabelled
  EUR.
- **`I000322`** — quarantined rather than imputed. Inventing €99 of revenue to keep a
  row is worse than losing the row.

Quarantine provides isolation and traceability today. In a larger system it is where
remediation workflows, upstream ownership alerts and reprocessing of corrected records
would attach — none of which are implemented here.

---

## Modelling

**Staging** renames, casts and normalizes, and collapses duplicate business keys. It
standardizes *format* and does not repair *values*: a blank country stays blank, a
negative price stays negative. Substituting a value the source never had is a business
decision and belongs in the mart that needs it.

**Intermediate** holds logic that more than one mart uses — the EUR revenue base, the
mart-eligible subscription set, the FX reference. Models that only one mart consumes
were left in that mart.

**Marts** answer the three questions in the brief:

| Model | Grain | Definition |
|---|---|---|
| `fct_mrr` | month × plan | sum of **paid** invoice amounts in EUR, attributed to the invoice month |
| `customer_ltv` | customer | total paid revenue, with country, plan mix, current status |
| `subscription_churn` | cancellation month | cancellations and MRR lost, from `monthly_price` |

MRR is recognised revenue, not contracted value. The brief asks for MRR from paid
invoices, so failed and open invoices contribute nothing. For a subscription paying on
time the two definitions agree; they diverge exactly where payment failed, which is the
point of using the paid basis.

`customer_ltv` uses LEFT JOINs throughout so customers whose subscriptions haven't
billed yet still appear with zero revenue rather than disappearing.

`subscription_churn` reports scheduled future cancellations with an
`is_future_cancellation` flag rather than filtering them — they are real commitments,
but shouldn't be mistaken for realised churn.

Both marts sum to the same €322,890.01, which a mart-level test asserts on every build.

Materializations follow current volume, not a rule:

```
staging, quarantine, intermediate → views
marts                             → tables
```

---

## FX assumption

Reporting currency is EUR. Conversion uses a **static rate mapping** in `int_fx_rates`
(`SEK → 0.087`), which the brief explicitly permits. Every invoice converts at the same
rate regardless of its date. Materially this affects one paid invoice, worth €26 of
€322,890.

The model is also a guard: `int_paid_invoices_eur` joins it with an INNER join, so a
currency with no rate cannot reach revenue at face value — it disappears, and the
reconciliation test fails loudly.

Static rates are a take-home simplification, not a design. Production alternative is in
Next Steps.

---

## Orchestration

One DAG, [`nordstack_analytics`](airflow/dags/nordstack_analytics.py), on the
five-minute schedule the brief asks for, emailing on success and failure.

```
dlt_sync  →  validate_raw  →  dbt_build  →  email
```

Three tasks rather than one script, so a failure names itself: a red `dlt_sync` is an
ingestion problem, a red `validate_raw` means ingestion reported success while the
warehouse disagrees, and a red `dbt_build` is a modelling or data-quality problem.
Airflow also retries only the task that failed.

| Setting | Value | Reason |
|---|---|---|
| `retries` | 2, 30s apart | transient infrastructure errors only |
| `execution_timeout` | 4 min | a task outliving the interval is stuck, not slow |
| `dagrun_timeout` | 4 min | under the schedule interval, so runs can't pile up |
| `catchup` | `False` | nothing is gained by backfilling five-minute intervals |
| `max_active_runs` | 1 | two runs replacing the same tables would race |
| `sla` | 2 / 3 / 4 min | lateness is the first symptom of overlap |

Retries are for transient failures. Broken SQL, a schema change or a failed trusted
contract fails identically every attempt and should stay visible rather than spin.

Two things this caught. `PythonOperator` marks a task successful unless the callable
*raises* — both ingestion scripts are CLIs whose `main()` **returns** an exit code, so
calling them directly reports a failed load as a green task; an adapter converts the code
into an exception. And `validate_raw` deliberately repeats a check the sync script
already performs, because a loader that reports success while leaving the warehouse wrong
is exactly what a self-check cannot catch. Both verified by breaking them on purpose: a
dead `MYSQL_HOST`, and a row deleted from `raw.customers`.

### Environment isolation

Only one database exists, so targets separate by **schema**. `dbt_build` resolves its
target per run: the run's `dbt_target` param, then `DBT_TARGET`, then `dev`.

```
scheduled run / airflow dags test  →  dev
Trigger DAG w/ config → prod       →  prod
```

`dev` is the default and the only fallback, so local DAG testing cannot reach
production; `prod` has to be selected. The resolved target is logged, pushed to XCom and
included in the notification email. Credentials come from environment configuration —
`profiles.yml` reads everything through `env_var()` and contains no literal values.

---

## CI/CD

Two workflows, dbt-focused. Every connection setting comes from repository variables and
secrets.

| Workflow | Trigger | Builds | Target |
|---|---|---|---|
| [`dbt-ci.yml`](.github/workflows/dbt-ci.yml) | pull request | `state:modified+` | `dev` |
| [`dbt-cd.yml`](.github/workflows/dbt-cd.yml) | merge to `main`, or manual | everything | `prod` |

CI runs only when `dbt/`, `ingestion/` or `seed_data/` change — an Airflow-only commit
builds no warehouse. Within that, `state:modified+` selects changed models plus
everything downstream; the `+` is what stops a break hiding behind an untouched
consumer. Verified on a real PR: editing `stg_invoices` rebuilt `int_paid_invoices_eur`,
`fct_mrr` and `customer_ltv`, and correctly skipped `subscription_churn`.

CD builds everything. A deployment must leave production internally consistent, and
every test must pass against what is published — not only against what changed in that
commit. `DBT_TARGET=prod` is set in exactly one place, the CD workflow. CI leaves it
unset, so a pull request cannot write to production.

**The honest cost of Slim CI here.** `--defer --state` needs a manifest to diff against
and real tables for unmodified models to resolve to. A GitHub runner has neither, so CI
bootstraps MySQL, syncs to Postgres and builds the base branch first to have something to
defer against. On a measured run that baseline costs 5s, the selective build 5s, and dbt
is 10s of 1m51s total — selective building costs more than it saves at this scale. It
stays because the substitution is one step: with a persistent warehouse the baseline
disappears and `--state` points at the manifest CD already publishes.

This is CI, not CD in the deployment sense — there is no persistent platform to deploy
to. A real environment would promote ingestion, dbt and DAG changes after merge.

---

## Key decisions

| Decision | Choice | Reason |
|---|---|---|
| Source boundary | MySQL → dlt → Postgres | exercises real ingestion, not seeding |
| Write disposition | `replace` | small static dataset; preserves planted defects |
| Raw tests | warn | detect source defects without blocking the build |
| Quarantine | built from raw | isolate invalid records before staging normalizes them |
| Staging | format only, no repair | value substitution is a business decision |
| Mart tests | error | the published contract must hold |
| Deduplication | dbt staging | visible and tested, not a side effect of ingestion |
| FX | static mapping | permitted by the brief; dated rates are a production concern |
| Default dbt target | `dev` | production must be selected, never inherited |
| CI selection | `state:modified+` with baseline | demonstrates the pattern at negligible cost |

---

## Next steps

Deferred deliberately — current volume doesn't justify them.

**Ingestion.** Move to `merge`/upsert on reliable business keys once full reloads get
expensive, then CDC from MySQL instead of scheduled extraction. Duplicate detection
would move from dbt staging to ingestion validation. Source keys and constraints belong
as close to the transactional model as possible.

**FX.** Replace static rates with a dated rate table (`date | currency | eur_rate`) fed
by its own ingestion, and convert each invoice at the rate for its accounting date.
Historical invoices should not be revalued at today's rate unless the business
explicitly defines it that way.

**Environments.** Persistent dev / CI / staging / prod. Per-developer dbt schemas, and a
staging environment for end-to-end validation before production.

**CI/CD.** Drop the baseline build once a production manifest exists. Add Airflow CI —
DAG imports, DAG ID, schedule, `catchup`, task dependencies — and ingestion CI covering
source contracts, schema evolution and a double-run idempotency check. Deploy only
changed components after merge.

**Materializations.** Tables for large reused intermediates; incremental for large
frequently rebuilt marts, once measurements justify it — with the questions incremental
brings (`unique_key`, late-arriving data, lookback windows, full-refresh strategy), none
of which are worth answering at 3k rows.

**Observability.** Source freshness, SLA alerting, ingestion reconciliation, structured
metrics, centralized logs.

**Governance.** Least-privilege service accounts, secrets management, PII classification,
audited access, model ownership, stronger schema contracts.

---

## Closing

The dataset is small, so the implementation stays simple — which doesn't have to mean
undisciplined. Raw keeps its defects visible, known-invalid records are isolated before
they reach the trusted path, marts enforce contracts that fail the build, retries are
bounded, dev and prod stay separate, and CI validates changes before merge.

Those boundaries are the part that scales: ingestion strategy, materializations,
observability and CI/CD can each evolve without disturbing the others.
