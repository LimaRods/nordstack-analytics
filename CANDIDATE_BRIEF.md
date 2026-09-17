# Data Engineer — Take-Home Assessment

**Time budget: ~3–4 hours.** We respect your time. If you hit the limit, stop and write down what you would do next in your README — a well-reasoned "next steps" section is worth more to us than gold-plating.

## Scenario

You've joined **NordStack**, a fictional B2B SaaS company selling subscription plans (`starter`, `growth`, `scale`) across Europe. The product team has handed you three raw CSV exports from the billing system and asked for a trustworthy analytics layer. Like all real billing exports, the data is not clean — treat it with appropriate suspicion.

You are given, in `seed_data/`:

| File | Grain | Notes |
|---|---|---|
| `raw_customers.csv` | one row per customer | ~120 rows |
| `raw_subscriptions.csv` | one row per subscription | a customer can have several |
| `raw_invoices.csv` | one row per monthly invoice | tied to a subscription |

A `docker-compose.yml` is provided to spin up Postgres 16 (`localhost:5432`, db `analytics`, user `dbt_user`, password `dbt_password`). You may use your own Postgres setup instead if you prefer — just document it.

## Your task

### 0. OPTIONAL Create a Data Sync Pipeline using dlt
_Note: THIS IS AN OPTIONAL STEP.__

Create a MySQL database (by extending provide docker-compose). Using the `dlt` (https://dlthub.com/product/dlt) tool create a data pipeline that syncs the tables from MySQL to PostgreSQL.


### 1. Create and seed the database (Postgres)

_If you created the dlt pipeline from step 0, you don't need to seed PostgrSQL and you can skip this step._

Load the three raw CSVs into Postgres. How you do this is up to you — `dbt seed`, `COPY` statements, a small script — but it must be **reproducible from a clean clone**: one or two documented commands should take a reviewer from nothing to a seeded database.

### 2. Build a dbt project

Model the data into an analytics layer. We expect at minimum:

- **Staging models** — one per source table: renaming, typing, and cleaning. Decide explicitly what to do with bad records (fix, quarantine, or exclude) and document the decision.
- **Marts / analysis models** — produce models that can answer:
  1. **Monthly Recurring Revenue (MRR) by month and by plan** — from paid invoices, in a single reporting currency (state your FX assumption; a hardcoded rate with a comment is fine).
  2. **Customer lifetime value to date** — total paid revenue per customer, with their country, plan mix, and current subscription status.
  3. **Churn view** — for each month, how many subscriptions were cancelled and the MRR lost.

Materialization choices (view vs. table), layer naming, and folder structure are yours — we're interested in *why*, so leave short comments or notes in your README.

### 3. Prove data quality with `dbt test`

This is a first-class part of the assessment, not an afterthought:

- Add **generic tests** (uniqueness, not-null, accepted values, relationships) where they belong.
- Add at least **two singular (custom SQL) tests** or custom generic tests that encode real business rules — think about what must *never* be true in billing data.
- The raw data contains **deliberately planted quality issues**. Your tests on the raw/staging layer should *catch* them; your marts should be built so tests on the marts *pass*. `dbt build` on your final project must exit green — with the failures handled upstream, not by deleting tests.
- In your README, list the data-quality issues you found and how each was handled.

### 4. Documentation

- A `README.md` with: setup steps, project structure, key modeling decisions, data-quality findings, and what you'd do next with more time.
- dbt `description:` fields on your marts and their key columns. Running `dbt docs generate` should produce useful documentation.


### 5. Airflow Datapipeline
Create an Airflow DAG that runs dbt project periodically (every 5 mins). The DAG should send a notification using Email when the DAG succeeds or fails.

## What to submit

A git repository (or zip) containing
- the dbt project,
- seeding mechanism
- `docker-compose.yml` (if modified with MySQL)
- Airflow DAG code (python files are enough)
- dlt code (if you picked optional step 0)
- and the README.
- Include your `packages.yml` if you use packages — `dbt_utils` and similar are welcome.

