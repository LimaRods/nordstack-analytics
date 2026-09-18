# Connection Details

Everything needed to connect to the project's services from a GUI client or the CLI.

> **These are local development defaults for a throwaway Docker stack — not secrets.**
> The Postgres values come from `CANDIDATE_BRIEF.md`; the MySQL values are ours to choose,
> since the brief only asks that a MySQL source be created. They live in `.env.example`
> (committed) and `.env` (git-ignored). Real credentials never belong in this file.

---

## Prerequisite

Both databases run as Docker containers. Nothing is installed on macOS directly.

```bash
cd "/Users/rodolfo/Desktop/Projects/Data-Engineering Projects/DE-Assessment "
docker compose up -d
docker compose ps          # both must show "healthy" before connecting
```

---

## PostgreSQL 16 — analytics warehouse

Destination for dlt (`raw` schema) and the database dbt builds on.

| Field | Value |
|---|---|
| Host | `127.0.0.1` |
| Port | `5432` |
| Database | `analytics` |
| Username | `dbt_user` |
| Password | `dbt_password` |

**pgAdmin:** Register → Server → *Connection* tab, fill the above. Any name on the *General* tab.

**CLI:**

```bash
docker compose exec postgres psql -U dbt_user -d analytics
```

Schemas currently present: `raw` (created by dlt). dbt will add its own.

---

## MySQL 8.0 — simulated billing source

The fictional operational system. Populated from `seed_data/*.csv` by the bootstrap script.

| Field | Value |
|---|---|
| Hostname | `127.0.0.1` |
| Port | `3306` |
| Default Schema | `billing` |
| Username | `billing_user` |
| Password | `billing_password` |
| Root (rarely needed) | `root` / `root_password` |

**MySQL Workbench:** *Connection Name* is a free label (e.g. `nordstack-mysql`); fill Hostname,
Port, Username, and set Default Schema to `billing`.

> Use `127.0.0.1`, **not** `localhost`. MySQL clients treat `localhost` as "use a Unix socket
> file", which does not exist because the server runs inside Docker. `127.0.0.1` forces TCP.
> (Postgres clients have no such quirk — either value works there.)

**CLI:**

```bash
docker compose exec mysql mysql -u billing_user -pbilling_password billing
```

---

## Host names differ inside Docker

From your Mac, both services are on `127.0.0.1`. From **inside** a container on the
`nordstack_default` network, they are reachable by service name:

| Connecting from | MySQL host | Postgres host |
|---|---|---|
| macOS (pgAdmin, Workbench, local Python) | `127.0.0.1` | `127.0.0.1` |
| Another container (Airflow, Phase 12) | `mysql` | `postgres` |

This is why connection settings are read from environment variables rather than hardcoded —
the same code works in both places with different configuration.

---

## Environment variables

Application code reads these; nothing is hardcoded. Copy the template once:

```bash
cp .env.example .env
```

| Variable | Default | Used by |
|---|---|---|
| `MYSQL_HOST` / `MYSQL_PORT` | `localhost` / `3306` | bootstrap, dlt sync |
| `MYSQL_DATABASE` / `MYSQL_USER` / `MYSQL_PASSWORD` | `billing` / `billing_user` / `billing_password` | bootstrap, dlt sync |
| `MYSQL_ROOT_PASSWORD` | `root_password` | container init only |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | dlt sync, dbt |
| `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | `analytics` / `dbt_user` / `dbt_password` | dlt sync, dbt |
| `DBT_TARGET` | `dev` | dbt, Airflow |

---

## dlt pipeline state

Not a connection, but useful to know where it lives: `~/.dlt/pipelines/nordstack_billing/`
(outside the repo, so nothing is committed).

```bash
./venv/bin/dlt pipeline nordstack_billing info          # state, dataset, schema
./venv/bin/dlt pipeline nordstack_billing trace         # per-step timings
./venv/bin/dlt pipeline nordstack_billing load-package  # last load's jobs
```

---

## Coming later

Placeholders to fill as the project progresses.

### dbt (Phase 5)

Profile will live in `dbt/profiles.yml`, reading the `POSTGRES_*` variables above.
Targets: `dev` (default, safe) and `prod` (explicit only). Both use the `analytics`
database and separate by **schema**, since only one database exists.

_To document: profile name, target schemas, `dbt debug` command._

### Airflow

`AIRFLOW_HOME` points at `./airflow` (set in `.env`) so the DAG folder is version
controlled instead of living in `~/airflow`. Runtime state — `airflow.db`, `airflow.cfg`,
`logs/` — is git-ignored.

```bash
export AIRFLOW_HOME="$PWD/airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
source .env

venv/bin/airflow db migrate          # once
venv/bin/airflow standalone          # UI on http://localhost:8080
```

`standalone` prints a generated admin password on first run and writes it to
`airflow/simple_auth_manager_passwords.json.generated` (git-ignored).

Test without the scheduler:

```bash
venv/bin/airflow dags test nordstack_analytics             # whole DAG, fires callbacks
venv/bin/airflow tasks test nordstack_analytics dbt_build  # one task
```

#### Email notifications

Set in `.env`, never committed. Gmail requires an **app password** (Google Account →
Security → 2-Step Verification → App passwords), not the account password.

| Variable | Purpose |
|---|---|
| `SMTP_HOST` / `SMTP_PORT` | `smtp.gmail.com` / `587` |
| `SMTP_USER` | account that authenticates and appears as sender |
| `SMTP_PASSWORD` | 16-character app password |
| `ALERT_EMAIL_TO` | comma-separated recipients |

The DAG sends through `smtplib` rather than Airflow's SMTP configuration, so the whole
notification path is in the repository rather than in machine-local `airflow.cfg`. If
these are unset the callback logs a warning and returns — a failure callback that itself
fails would hide the error it was reporting.

---

## CI configuration (GitHub Actions)

`.github/workflows/dbt-ci.yml` hardcodes nothing. Before the first run, set these in
**Settings → Secrets and variables → Actions**. The values are the ones in
`.env.example`; they live in repository settings so that pointing CI at a different
database is a settings change rather than a code change.

### Variables tab

| Variable | Value |
|---|---|
| `POSTGRES_HOST` | `127.0.0.1` |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `analytics` |
| `POSTGRES_USER` | `dbt_user` |
| `MYSQL_HOST` | `127.0.0.1` |
| `MYSQL_PORT` | `3306` |
| `MYSQL_DATABASE` | `billing` |
| `MYSQL_USER` | `billing_user` |

### Secrets tab

| Secret | Value |
|---|---|
| `POSTGRES_PASSWORD` | `dbt_password` |
| `MYSQL_PASSWORD` | `billing_password` |
| `MYSQL_ROOT_PASSWORD` | `root_password` |

Set them with the `gh` CLI in one go:

```bash
gh variable set POSTGRES_HOST --body "127.0.0.1"
gh variable set POSTGRES_PORT --body "5432"
gh variable set POSTGRES_DB   --body "analytics"
gh variable set POSTGRES_USER --body "dbt_user"
gh variable set MYSQL_HOST     --body "127.0.0.1"
gh variable set MYSQL_PORT     --body "3306"
gh variable set MYSQL_DATABASE --body "billing"
gh variable set MYSQL_USER     --body "billing_user"

gh secret set POSTGRES_PASSWORD   --body "dbt_password"
gh secret set MYSQL_PASSWORD      --body "billing_password"
gh secret set MYSQL_ROOT_PASSWORD --body "root_password"
```

> Secrets are not exposed to workflows triggered by pull requests **from forks**. For a
> fork-based contribution the job will fail on connection rather than silently running
> against the wrong database — which is the correct failure mode.
