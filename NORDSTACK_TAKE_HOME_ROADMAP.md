# NordStack Data Engineer Take-Home — Implementation Roadmap

> **Primary goal:** Complete every required part of the assessment using the optional `dlt` ingestion path, while keeping the implementation small, reproducible, and production-minded.
>
> **Scope discipline:** Do not gold-plate. Implement the required pipeline plus two deliberate extras:
>
> 1. End-to-end CI for `dlt` + dbt + Airflow DAG validation.
> 2. Production-minded reliability: idempotency, consistency, retries, failure handling, validation, governance, and basic SLA awareness.
>
> Scaling optimizations, advanced materializations, richer observability, RBAC, staging environments, and infrastructure improvements belong in **Next Steps**, not the first implementation.

---

## 1. Target Architecture

```text
seed_data/*.csv
      |
      | bootstrap source system once
      v
+------------------+
|      MySQL       |
| simulated source |
+--------+---------+
         |
         | dlt SQL source
         | merge by primary key
         v
+------------------+
|    PostgreSQL    |
|       raw        |
+--------+---------+
         |
         | dbt
         v
+------------------------------+
| staging                      |
| clean + type + standardize   |
| invalid rows -> quarantine   |
+--------------+---------------+
               |
               v
+------------------------------+
| intermediate                 |
| reusable business logic      |
+--------------+---------------+
               |
               v
+------------------------------+
| marts                        |
| MRR | LTV | Churn            |
+------------------------------+
```

Airflow orchestrates the recurring production-like flow:

```text
dlt_sync
   |
   v
validate_sync
   |
   v
dbt_build
   |
   +------------------+
   |                  |
success email      failure email
```

CI validates the same system from a clean environment:

```text
Pull Request
   |
   v
Docker services
   |
   v
Bootstrap MySQL
   |
   v
Run dlt twice
   |
   v
Idempotency checks
   |
   v
dbt build
   |
   v
Airflow DAG import/tests
   |
   v
GREEN / BLOCK PR
```

---

# 2. Core Design Principles

Keep these principles visible throughout implementation and presentation.

## 2.1 Raw preserves source semantics

The `raw` schema should be populated by `dlt`, not by dbt transformation logic.

Do not clean business issues during ingestion unless a record is structurally impossible to load.

Purpose:

- Preserve what arrived from the source.
- Make data-quality problems observable.
- Keep ingestion separate from analytics transformation.

---

## 2.2 Staging establishes trust

Staging models should:

- Rename columns consistently.
- Cast data types.
- Standardize categorical values.
- Normalize timestamps.
- Flag invalid rows.
- Exclude or quarantine records that must not enter analytical marts.

Do not silently drop invalid rows without documenting why.

---

## 2.3 Marts encode business meaning

The marts answer exactly the business questions requested:

1. MRR by month and plan.
2. Customer lifetime value to date.
3. Monthly churn and MRR lost.

Avoid adding unrelated marts.

---

## 2.4 Tests are executable contracts

Use:

- Generic tests for structural contracts.
- Custom/singular tests for business invariants.
- Diagnostic handling for deliberately bad raw data.
- Error-level tests for cleaned staging and marts.

The final `dbt build` must finish successfully.

---

## 2.5 Every retriable operation must be idempotent

A retry should not create duplicate business rows.

Examples:

- Re-running the MySQL bootstrap should result in the same source tables.
- Re-running `dlt` should not duplicate customers, subscriptions, or invoices.
- Re-running dbt should deterministically rebuild the same analytical result.

---

## 2.6 Retry transient failures; fail deterministic failures

Retries are useful for:

- Temporary database connectivity issues.
- Network interruptions.
- Temporary resource failures.

Retries do not fix:

- Broken SQL.
- Missing required columns.
- Invalid business rules.
- Failed data-quality contracts.

Deterministic failures should fail clearly and trigger notification.

---

# 3. Suggested Repository Structure

```text
.
├── README.md
├── IMPLEMENTATION_ROADMAP.md
├── docker-compose.yml
├── .env.example
├── .gitignore
├── requirements.txt
│
├── seed_data/
│   ├── raw_customers.csv
│   ├── raw_subscriptions.csv
│   └── raw_invoices.csv
│
├── ingestion/
│   ├── bootstrap_mysql.py
│   └── sync_mysql_to_postgres.py
│
├── dbt_nordstack/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── macros/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_subscriptions.sql
│   │   │   ├── stg_invoices.sql
│   │   │   └── staging.yml
│   │   ├── quarantine/
│   │   │   ├── invalid_customers.sql
│   │   │   ├── invalid_subscriptions.sql
│   │   │   └── invalid_invoices.sql
│   │   ├── intermediate/
│   │   │   └── ...
│   │   └── marts/
│   │       ├── fct_mrr.sql
│   │       ├── customer_ltv.sql
│   │       ├── subscription_churn.sql
│   │       └── marts.yml
│   └── tests/
│       ├── assert_paid_invoice_positive_amount.sql
│       └── assert_valid_subscription_dates.sql
│
├── airflow/
│   └── dags/
│       └── nordstack_analytics.py
│
├── tests/
│   ├── test_ingestion_idempotency.py
│   └── test_airflow_dag.py
│
└── .github/
    └── workflows/
        └── ci.yml
```

Do not force every suggested file if the data does not justify it.

---

# 4. Phase 0 — Inspect the Supplied Data First

Before writing models:

1. Inspect all CSV headers.
2. Identify the real primary key for each entity.
3. Check:
   - null IDs
   - duplicates
   - unexpected statuses
   - invalid plans
   - currencies
   - negative/zero invoice amounts
   - impossible date ordering
   - orphan subscriptions
   - orphan invoices
   - cancelled subscriptions
   - multiple subscriptions per customer
4. Write down every discovered issue.

Create a small decision table for yourself:

| Issue | Layer detected | Handling |
|---|---|---|
| duplicate business key | raw/staging | deduplicate or quarantine |
| invalid plan | staging | quarantine |
| orphan invoice | staging | quarantine |
| paid invoice <= 0 | staging | quarantine |
| malformed date | staging | quarantine |
| inconsistent casing | staging | normalize |

### Done when

You can state:

- the grain of every raw table;
- the primary key of every table;
- every planted quality issue you found;
- how each issue will be handled.

---

# 5. Phase 1 — Docker Infrastructure

Extend the provided Docker Compose with MySQL.

Minimum services:

```text
mysql
postgres
airflow
```

Airflow may require its own metadata database depending on the provided setup. Keep the topology as simple as possible.

Use health checks for MySQL and PostgreSQL so dependent services do not start blindly.

Use environment variables for connections.

Example configuration categories:

```text
MYSQL_HOST
MYSQL_PORT
MYSQL_DATABASE
MYSQL_USER
MYSQL_PASSWORD

POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

Commit `.env.example`, not secrets.

The credentials supplied by the exercise can be defaults for local development, but application code should read them from environment variables.

### Production-minded decision

**Governance starts with separation of configuration from code.**

Do not scatter connection strings across Python, dbt, and Airflow files.

### Done when

From a clean clone:

```bash
docker compose up -d
```

starts healthy MySQL and PostgreSQL instances.

---

# 6. Phase 2 — Bootstrap the Simulated MySQL Source

The assessment gives CSV files, but Step 0 expects MySQL to behave as the source system.

Create:

```text
ingestion/bootstrap_mysql.py
```

Responsibility:

```text
CSV -> MySQL source tables
```

This is **not** the analytics ingestion pipeline. It only initializes the fictional billing source database for the assessment.

Recommended behavior:

1. Read each CSV.
2. Create/replace the corresponding MySQL table.
3. Preserve raw values as much as possible.
4. Fail if a required input file is missing.
5. Log row counts.

For this static take-home source, `replace`/truncate-and-load behavior is acceptable because the bootstrap script represents source initialization.

Make the operation idempotent:

```text
run bootstrap once  -> 120 customers
run bootstrap again -> 120 customers
not 240
```

### Done when

A clean MySQL database can be created reproducibly from the supplied CSV files with one documented command.

---

# 7. Phase 3 — Build the dlt MySQL -> PostgreSQL Sync

This is the part to keep deliberately simple.

## 7.1 Mental model

`dlt` is responsible for:

```text
EXTRACT from MySQL
      +
NORMALIZE
      +
LOAD into PostgreSQL
```

dbt starts **after** the data is in PostgreSQL.

Use the dlt SQL database source and PostgreSQL destination.

Conceptually:

```python
source = sql_database(mysql_connection)

pipeline = dlt.pipeline(
    pipeline_name="nordstack_billing",
    destination="postgres",
    dataset_name="raw",
)

load_info = pipeline.run(source)
```

---

## 7.2 Use merge, not blind append

Do not use default append semantics for these entity tables.

Configure each resource with:

```text
write_disposition = merge
primary_key       = business primary key
```

Expected mapping after inspecting actual columns:

```text
customers     -> customer primary key
subscriptions -> subscription primary key
invoices      -> invoice primary key
```

Why:

```text
Airflow run 1 -> invoice_123 inserted
Airflow retry -> invoice_123 matched
             -> updated/kept
             -> NOT duplicated
```

That is the core idempotency guarantee you want to demonstrate.

---

## 7.3 Treat dlt failure as pipeline failure

Your ingestion script should:

1. Run the dlt pipeline.
2. Print/log `LoadInfo`.
3. Raise if dlt reports failed jobs.
4. Exit non-zero on failure.

Do not swallow exceptions.

This ensures Airflow can correctly mark the task as failed and apply its retry policy.

---

## 7.4 Basic ingestion validation

After load, validate at minimum:

- destination tables exist;
- destination PK is not null;
- no duplicate business primary keys;
- expected source tables were loaded.

For this tiny static exercise, a row-count comparison between MySQL and PostgreSQL is also useful.

Do not build a complex reconciliation framework.

### Done when

This sequence succeeds:

```bash
python ingestion/bootstrap_mysql.py
python ingestion/sync_mysql_to_postgres.py
python ingestion/sync_mysql_to_postgres.py
```

and the second dlt run does **not** duplicate business rows.

That double-run is your simplest practical proof of idempotency.

---

# 8. Phase 4 — Configure dbt Sources

Declare the three dlt-loaded PostgreSQL tables as dbt sources.

Example logical structure:

```yaml
sources:
  - name: billing_raw
    schema: raw
    tables:
      - name: customers
      - name: subscriptions
      - name: invoices
```

Use source descriptions.

Add relevant generic tests, but be careful with deliberately bad raw data.

---

# 9. Phase 5 — Data Quality Strategy

This is a first-class part of the submission.

Use two levels of quality enforcement.

## 9.1 Raw diagnostics

Raw data is allowed to contain source defects.

Tests that intentionally identify planted bad source records can be:

- warnings;
- stored failures;
- or explicit diagnostic/quarantine models.

The goal is:

```text
bad source data is visible
        +
final dbt build remains green
```

Do not remove tests simply because they find bad records.

---

## 9.2 Clean analytical contract

After staging/quarantine handling, staging and marts should satisfy error-level contracts.

Examples:

### Generic tests

```text
unique
not_null
accepted_values
relationships
```

Apply them where they express a real invariant.

Examples:

- customer ID unique + not null;
- subscription ID unique + not null;
- invoice ID unique + not null;
- plan accepted values: starter/growth/scale;
- subscription customer relationship;
- invoice subscription relationship;
- expected status accepted values.

---

## 9.3 Required custom business-rule tests

Implement at least two meaningful tests.

Good candidates:

### Test A — paid revenue must be economically valid

```text
A paid invoice must never have non-positive normalized revenue.
```

### Test B — subscription dates must be chronologically valid

```text
cancelled_at must never precede started_at.
```

Other valid candidates:

```text
paid invoice must have payment timestamp
MRR must never be negative
active subscription must not have a cancellation date
invoice currency must be supported
```

Choose rules supported by the actual supplied columns.

---

# 10. Phase 6 — Staging and Quarantine Models

Create one staging model per raw table.

Target responsibilities:

## `stg_customers`

- standardized names;
- typed identifiers;
- normalized country;
- clean timestamps;
- explicit validity flags or exclusion logic.

## `stg_subscriptions`

- normalized plan;
- normalized status;
- typed dates;
- verify start/cancellation chronology;
- verify customer relationship.

## `stg_invoices`

- normalized status;
- typed amount;
- normalized currency;
- typed invoice/payment dates;
- verify subscription relationship;
- identify economically invalid paid invoices.

---

## Quarantine pattern

Prefer:

```text
RAW
 |
 +--> valid rows   -> staging
 |
 +--> invalid rows -> quarantine
```

over:

```text
RAW -> WHERE bad_condition = false
```

with no trace of excluded records.

The quarantine models give the reviewer an auditable answer to:

> "What happened to the bad records?"

Keep quarantine simple. It does not need a workflow for manual remediation.

---

# 11. Phase 7 — Intermediate Models

Only introduce intermediate models where logic is genuinely reusable.

Likely useful examples:

```text
int_paid_invoices
int_invoice_revenue_eur
int_subscription_revenue
int_customer_subscription_summary
```

Do not create layers just because the folder exists.

A good rule:

> If business logic is reused by more than one mart, consider moving it to intermediate.

---

# 12. Phase 8 — FX Decision

Use **EUR** as the single reporting currency because NordStack operates across Europe.

For the assignment, keep FX intentionally simple.

Options:

### Preferred simple option

Create one small reusable FX mapping in SQL or a tiny reference model.

```text
EUR -> 1.00
GBP -> documented fixed rate
USD -> documented fixed rate
...
```

Document clearly:

> Static FX rates are used for the assessment. A production implementation would use dated historical FX rates and convert each invoice using the applicable rate for its accounting date.

Do not build an FX API.

---

# 13. Phase 9 — Build the Required Marts

## 13.1 MRR by month and plan

Required grain:

```text
month + plan
```

Source:

```text
paid invoices only
```

Metric:

```text
SUM(revenue normalized to EUR)
```

Document the exact definition of "MRR" used for the exercise.

Because the prompt explicitly asks to derive it from paid invoices, follow that requirement rather than inventing subscription contract MRR logic.

---

## 13.2 Customer lifetime value to date

Required grain:

```text
one row per customer
```

Include:

- customer ID;
- country;
- total paid revenue in EUR;
- plan mix;
- current subscription status.

Define "current subscription status" explicitly.

If customers may hold several subscriptions, choose a deterministic rule, for example:

```text
active if any subscription is active,
otherwise status of the most recently started/updated subscription
```

Adjust after inspecting actual data.

---

## 13.3 Churn view

Required grain:

```text
month
```

Metrics:

```text
cancelled subscriptions
MRR lost
```

Preferred logic:

- month = cancellation month;
- count cancelled subscription IDs;
- MRR lost = recurring value associated with those subscriptions.

If the subscriptions dataset contains recurring price, use it.

If not, document a deterministic proxy such as the most recent paid monthly invoice before cancellation.

Do not hide ambiguity: state the chosen assumption.

---

# 14. Phase 10 — Materialization Strategy

Keep it simple for the current data volume.

Recommended:

```text
staging      -> view
quarantine   -> view
intermediate -> view or ephemeral
marts        -> table
```

Reasoning:

- staging is lightweight cleanup;
- intermediate logic is small;
- marts are stable consumer-facing datasets.

Do **not** implement incremental marts merely to demonstrate that you know them.

Put the scaling decision in `Next Steps`:

```text
As volume grows:
table/incremental decisions should be revisited using model runtime,
scan volume, update patterns, and downstream query frequency.
```

---

# 15. Phase 11 — dbt Documentation

Add descriptions for:

- every mart;
- key mart columns;
- business metrics;
- important assumptions.

Run:

```bash
dbt docs generate
```

Descriptions should answer:

```text
What does this model represent?
What is its grain?
What does this metric mean?
What assumptions were used?
```

---

# 16. Phase 12 — Airflow DAG

Create one DAG:

```text
nordstack_analytics
```

Schedule:

```text
*/5 * * * *
```

Recommended tasks:

```text
sync_mysql_to_postgres
        |
        v
validate_raw_sync
        |
        v
dbt_build
```

This exceeds the minimum slightly but creates a coherent end-to-end pipeline.

---

## 16.1 Reliability settings

Suggested starting configuration:

```text
catchup = False
max_active_runs = 1
retries = 2
retry_delay = short
```

Also consider:

```text
dagrun_timeout < schedule interval
```

or task execution timeouts.

Why:

- `catchup=False`: the exercise does not need historical five-minute backfills.
- `max_active_runs=1`: prevents two copies of the pipeline from concurrently modifying the same small destination.
- retries: recover from transient failures.
- timeout/deadline awareness: prevent one stuck run from silently violating the five-minute operating target.

---

## 16.2 Self-healing behavior

Expected behavior:

```text
temporary DB failure
      |
      v
Airflow retry
      |
      v
dlt merge resumes/reruns safely
      |
      v
no duplicate business rows
```

Do not describe every failure as self-healing.

If dbt fails because a business-rule test detects invalid analytical data:

```text
FAIL
 |
 +-> do not publish downstream success
 |
 +-> notify
```

That is correct behavior.

---

## 16.3 Email notification

Configure DAG-level:

```text
on_success_callback
on_failure_callback
```

Use Airflow email configuration / SMTP or supported email backend.

Credentials belong in environment configuration / Airflow connections, not DAG source code.

The assignment only asks for email on DAG success/failure. Do not build Slack/PagerDuty.

---

# 17. Phase 13 — CI/CD Extra

The practical deliverable should focus on **CI** because the take-home does not provide a real production deployment target.

Do not pretend that "merge to main" is a production deployment if there is nowhere to deploy.

Implement a complete CI workflow and document the intended CD promotion point.

---

## 17.1 GitHub Actions CI

Trigger:

```text
pull_request
push to main
```

Suggested order:

```text
1. Checkout
2. Install dependencies
3. docker compose up MySQL/Postgres
4. Wait for health checks
5. Bootstrap MySQL
6. Run dlt sync
7. Run dlt sync AGAIN
8. Assert idempotency
9. dbt deps
10. dbt debug
11. dbt build
12. dbt docs generate
13. Validate Airflow DAG imports
14. Run DAG unit tests
15. docker compose down
```

---

## 17.2 What CI protects

### dlt / ingestion

CI should fail for:

```text
pipeline exception
failed dlt load job
duplicate primary keys after retry
missing required raw table
```

### dbt

CI should fail for:

```text
SQL compile failure
model build failure
contract/test failure
broken relationships
mart quality failure
```

### Airflow

CI should fail for:

```text
DAG import error
missing expected DAG
wrong task dependency
invalid schedule configuration
Python syntax/import error
```

---

## 17.3 Airflow CI test

At minimum validate that Airflow can import every DAG without errors.

Optionally add a tiny unit test checking:

```text
DAG ID
schedule
catchup=False
expected task IDs
expected dependency order
```

Do not spin up a full distributed Airflow cluster inside CI unless it is already trivial with the supplied Compose setup.

---

# 18. Phase 14 — Idempotency Test

This is one of the strongest small additions to the submission.

Automate:

```text
bootstrap MySQL

run dlt
capture:
  customers count
  subscriptions count
  invoices count

run dlt again

capture counts again

assert:
  first counts == second counts
  business PKs remain unique
```

This directly demonstrates:

> "Retries and repeated scheduled execution do not double-write data."

Keep the test narrow.

---

# 19. Phase 15 — Data Consistency Checks

Do not build a full reconciliation service.

Implement enough to prove the concept.

Suggested invariants:

```text
COUNT(DISTINCT customer_pk) = COUNT(*) in raw customer destination
COUNT(DISTINCT subscription_pk) = COUNT(*)
COUNT(DISTINCT invoice_pk) = COUNT(*)

no accepted staging invoice references a missing staging subscription
no accepted staging subscription references a missing staging customer
```

dbt relationship tests can enforce the latter two.

For the static exercise source, source/destination counts can also be compared after dlt sync.

---

# 20. Phase 16 — Governance Without Overengineering

For this assignment, "data governance" should mean:

## Ownership and meaning

- descriptions;
- grain documented;
- business definitions documented;
- assumptions documented.

## Lineage

```text
MySQL -> dlt -> PostgreSQL raw -> dbt staging -> intermediate -> marts
```

dbt provides model lineage for the transformation layer.

## Access/configuration hygiene

- credentials outside code;
- `.env.example`;
- no secrets committed.

## Quality contracts

- source/staging tests;
- business-rule tests;
- quarantine;
- marts that only consume trusted rows.

Do not implement a catalog, IAM platform, row-level security system, or metadata service for this take-home.

---

# 21. Phase 17 — Observability and SLA Mindset

Use the tools already present.

## Observability

```text
dlt LoadInfo
Airflow task logs
Airflow DAG state
dbt build/test output
email success/failure
```

Do not build Grafana.

## SLA / reliability objective

The assessment asks for execution every five minutes.

A reasonable documented target:

```text
Schedule: every 5 minutes
No overlapping DAG runs
Transient failures retried automatically
Persistent failure produces email notification
A stuck run is bounded by timeout/deadline
```

Do not invent enterprise SLA percentages unless asked.

---

# 22. Phase 18 — README Structure

The final `README.md` should be optimized for a reviewer.

Recommended order:

## 1. Overview

One paragraph explaining the project.

## 2. Architecture

One small diagram:

```text
MySQL -> dlt -> Postgres RAW -> dbt -> Marts
                  ^
                  |
               Airflow
```

Add CI separately.

## 3. Quick Start

Aim for very few commands.

Example goal:

```bash
cp .env.example .env
docker compose up -d
python ingestion/bootstrap_mysql.py
python ingestion/sync_mysql_to_postgres.py
cd dbt_nordstack && dbt build
```

If possible, wrap this into:

```bash
make setup
make build
```

but only if adding a Makefile is fast.

## 4. Project Structure

Explain folders briefly.

## 5. Modeling Decisions

Explain:

```text
staging = views
intermediate = reusable logic
marts = tables
EUR reporting currency
quality/quarantine policy
```

## 6. Data Quality Findings

Table:

| Issue | Detection | Resolution |
|---|---|---|
| ... | ... | ... |

## 7. Reliability / Production Mindset

Briefly explain:

- dlt merge;
- idempotent reruns;
- Airflow retry policy;
- no overlapping runs;
- quality gates;
- email notification;
- CI.

## 8. Assumptions

Explicitly document ambiguous business definitions.

## 9. Next Steps

Only here discuss:

- incremental dbt models;
- historical FX;
- larger data volumes;
- partitioning/indexing;
- staging environment;
- production RBAC;
- richer observability;
- freshness alerts;
- backfills;
- CDC;
- schema contracts;
- infrastructure as code.

---

# 23. Build Order — Do This in This Sequence

This is the actual working checklist.

## Milestone A — Data discovery

- [ ] Inspect all CSVs.
- [ ] Identify primary keys.
- [ ] Identify planted quality issues.
- [ ] Decide handling for every issue.
- [ ] Write assumptions.

## Milestone B — Infrastructure

- [ ] Add MySQL to Docker Compose.
- [ ] Confirm PostgreSQL connectivity.
- [ ] Add environment configuration.
- [ ] Add health checks.

## Milestone C — Source bootstrap

- [ ] Build `bootstrap_mysql.py`.
- [ ] Load all three CSVs.
- [ ] Make bootstrap idempotent.
- [ ] Verify source row counts.

## Milestone D — dlt

- [ ] Configure MySQL SQL source.
- [ ] Configure PostgreSQL destination.
- [ ] Dataset/schema = `raw`.
- [ ] Select only three required tables.
- [ ] Set `merge`.
- [ ] Set correct primary key per table.
- [ ] Surface dlt failed jobs as process failures.
- [ ] Run twice.
- [ ] Verify no duplicates.

## Milestone E — dbt foundation

- [ ] Initialize dbt project.
- [ ] Configure Postgres profile with environment variables.
- [ ] Define sources.
- [ ] Create staging models.
- [ ] Create quarantine handling.
- [ ] Add generic tests.

## Milestone F — business tests

- [ ] Add custom business test 1.
- [ ] Add custom business test 2.
- [ ] Confirm planted issues are detected.
- [ ] Confirm accepted staging data passes.
- [ ] Ensure final `dbt build` is green.

## Milestone G — marts

- [ ] MRR by month + plan.
- [ ] Customer LTV.
- [ ] Churn + MRR lost.
- [ ] Normalize revenue to EUR.
- [ ] Document business assumptions.
- [ ] Add mart tests.
- [ ] Add mart descriptions.

## Milestone H — Airflow

- [ ] DAG every five minutes.
- [ ] dlt sync task.
- [ ] raw validation task.
- [ ] dbt build task.
- [ ] retries.
- [ ] no overlapping DAG runs.
- [ ] timeout/deadline awareness.
- [ ] success email.
- [ ] failure email.
- [ ] test DAG manually.

## Milestone I — CI

- [ ] GitHub Actions workflow.
- [ ] Start clean DB services.
- [ ] Bootstrap source.
- [ ] Run dlt twice.
- [ ] Idempotency assertion.
- [ ] Run dbt build.
- [ ] Generate docs.
- [ ] Check Airflow DAG imports.
- [ ] Run DAG unit tests.
- [ ] CI goes green.

## Milestone J — Reviewer experience

- [ ] README quick start works from clean clone.
- [ ] Architecture is visible.
- [ ] Data-quality findings are listed.
- [ ] Tradeoffs are stated.
- [ ] Production principles are concise.
- [ ] Next Steps are separated from implemented scope.

---

# 24. Acceptance Criteria

The project is ready to submit when all of these are true.

## Reproducibility

A reviewer can clone the repository and reproduce the environment using documented commands.

## Ingestion

```text
MySQL -> dlt -> PostgreSQL RAW
```

works.

Running ingestion more than once does not duplicate business records.

## Data quality

Planted source issues are visible and documented.

Invalid rows do not contaminate the marts.

The final:

```bash
dbt build
```

returns green.

## Analytics

The project contains working outputs for:

```text
MRR by month/plan
customer LTV
monthly churn + MRR lost
```

## Airflow

The DAG:

```text
runs every 5 minutes
retries transient failures
prevents overlapping runs
sends success/failure email
runs the required dbt project
```

## CI

A PR automatically validates:

```text
ingestion
idempotency
dbt
Airflow DAG import/structure
```

## Documentation

The README explains:

```text
setup
architecture
modeling
quality findings
reliability choices
assumptions
next steps
```

---

# 25. Decisions to Defend in the Presentation

Be ready to explain these.

### Why MySQL + dlt instead of dbt seed?

Because the optional task simulates a real source-to-warehouse ingestion boundary.

### Why dlt merge?

Because the Airflow job runs repeatedly and retries must not duplicate entity records.

### Why keep raw data dirty?

Because raw represents source state. Quality correction belongs in the transformation/trust layer and defects should remain auditable.

### Why quarantine?

Because silently deleting invalid rows makes operational defects invisible.

### Why views for staging?

Small data volume and lightweight cleanup do not justify duplicated storage.

### Why tables for marts?

They represent stable consumer-facing analytical datasets.

### Why not incremental dbt models?

The current dataset is tiny; incremental complexity has no performance justification yet.

### Why Airflow retries?

To recover automatically from transient infrastructure errors.

### Why not retry indefinitely?

Permanent code/data-quality failures require visibility, not endless retries.

### Why `max_active_runs=1`?

The pipeline is scheduled every five minutes; overlapping writes add unnecessary concurrency risk for this use case.

### Why CI?

Because code should prove ingestion safety, transformation correctness, and DAG validity before merge.

### Why no elaborate CD deployment?

The take-home does not provide a real deployment environment. CI is executable; the production deployment boundary is documented rather than faked.

---

# 26. Production Mindset in One Paragraph

Use this idea in your presentation, in your own words:

> The implementation keeps the system deliberately small while preserving production principles. Ingestion is repeatable and idempotent, source defects remain observable, staging establishes a trusted data contract, marts contain only validated business data, Airflow provides orchestration and bounded automatic recovery for transient failures, and CI exercises the pipeline from ingestion through dbt and DAG validation before code can be merged. The solution does not add scaling complexity until the current workload justifies it.

---

# 27. Explicitly Deferred to Next Steps

Do **not** implement these initially:

- incremental dbt materializations;
- CDC from MySQL;
- historical/daily FX API;
- full staging environment;
- per-PR PostgreSQL environments;
- Airflow HA architecture;
- Kubernetes;
- Terraform;
- secrets manager;
- advanced PostgreSQL indexing;
- partitioning;
- OpenTelemetry / Prometheus / Grafana;
- data catalog;
- automated quarantine remediation;
- complex RBAC;
- large-scale load/performance testing.

Mention them only when explaining how the architecture could evolve with data volume, team size, compliance requirements, or stricter operational SLAs.

---

## Final Rule

When deciding whether to add something, ask:

> Does this feature help satisfy the assignment, prove reliability, or make the system easier to review?

If the answer is no, put it in **Next Steps** instead of implementing it.
