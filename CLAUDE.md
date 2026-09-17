# CLAUDE.md — Engineering rules for the NordStack take-home

This file governs every implementation and modification in this repository.

## Authority order

When sources conflict, follow this order:

1. **`CANDIDATE_BRIEF.md`** — the assignment. Non-negotiable.
2. **`NORDSTACK_TAKE_HOME_ROADMAP.md`** — the agreed implementation plan.
3. **This file** — engineering principles.

Where this file deliberately diverges from the roadmap, the divergence is stated
with its reason (see [Known divergences](#known-divergences)). Do not silently
re-introduce what was deliberately dropped.

---

## Repository map

Authoritative paths. `dbt/` and `airflow/` already exist and supersede the
roadmap's illustrative `dbt_nordstack/` name.

```
.
├── CANDIDATE_BRIEF.md              assignment
├── NORDSTACK_TAKE_HOME_ROADMAP.md  roadmap
├── CLAUDE.md                       this file
├── README.md                       reviewer-facing documentation
├── docker-compose.yml              postgres (+ mysql, airflow to be added)
├── .env.example                    configuration contract — no secrets
├── requirements.txt
├── seed_data/*.csv                 supplied source extracts
├── ingestion/                      bootstrap_mysql.py, sync_mysql_to_postgres.py
├── dbt/                            dbt_project.yml, models/, tests/, macros/
│   └── models/{staging,quarantine,intermediate,marts}/
├── airflow/dags/                   nordstack_analytics.py
├── tests/                          test_ingestion_idempotency.py, test_airflow_dag.py
└── .github/workflows/ci.yml
```

### Open prerequisites

Two repo-level issues are unresolved and affect reproducibility:

1. **The project directory name ends in a space** (`DE-Assessment `). This breaks
   unquoted paths in shell scripts, Docker bind mounts, and CI checkouts. Quote
   every path until it is renamed.
2. **This is not a git repository.** The brief asks for a git repo or a zip, and
   the CI rules below assume pull-request triggers. `git init` plus a `.gitignore`
   (`.env`, `target/`, `dbt_packages/`, `logs/`, `__pycache__/`, `.dlt/`) is
   required before CI means anything.

---

## Ground truth: the supplied data

Verified against `seed_data/`. Do not reference a column that is not listed here.

| Table | Grain | Rows | PK | Columns |
|---|---|---|---|---|
| `raw_customers` | one customer | 121 | `customer_id` | `customer_id, customer_name, email, country, created_at` |
| `raw_subscriptions` | one subscription | 175 | `subscription_id` | `subscription_id, customer_id, plan_name, monthly_price, start_date, end_date, status` |
| `raw_invoices` | one monthly invoice | 2 855 | `invoice_id` | `invoice_id, subscription_id, invoice_date, amount, currency, status` |

Domains: plans `starter | growth | scale`; invoice status `paid | open | failed`;
subscription status `active | cancelled | paused`.

**There is no payment-timestamp column.** Cancellation is expressed by
`end_date`, not `cancelled_at`. `monthly_price` is present on subscriptions, so
churn MRR-lost needs no proxy metric.

### Planted defects — confirmed by profiling

These must remain detectable. None may be silently cleaned during ingestion.

| Defect | Evidence | Handled at |
|---|---|---|
| Duplicate business key | `C0023`, `S00006` | staging / quarantine |
| Orphan foreign key | sub `S00011` → missing customer; invoice `I000601` → missing subscription | staging `relationships` |
| Non-positive paid amount | `I000725`–`I000728` at `-99.00` | singular test A |
| Missing amount on a paid invoice | `I000322`, empty `amount` | staging |
| Negative monthly price | `S00048` at `-99.00` | staging |
| `end_date` before `start_date` | `S00034` (2025-03-29 → 2025-03-19) | singular test B |
| Case / whitespace variants | `'PAID '`, `'ACTIVE'` | staging normalization |
| Missing country | `C0008`, plus one blank | staging |
| Non-EUR currency | 2 `SEK` invoices vs 2 853 `EUR` | FX conversion |

---

## 1. Scope discipline

This is a **3–4 hour take-home**. The brief is explicit: a well-reasoned "next
steps" section is worth more than gold-plating.

Prefer the simplest implementation that satisfies the assignment, is
reproducible, is testable, and demonstrates production judgment.

Do not add infrastructure or abstractions to appear sophisticated. The following
stay **documented as future improvements** unless explicitly requested:
Kubernetes, Terraform, Kafka, Grafana/Prometheus/OpenTelemetry, elaborate RBAC,
production-scale CDC, secrets managers, data catalogs, distributed
infrastructure.

When choosing between complexity and clarity, choose clarity — unless the
complexity directly improves correctness, reliability, or an explicit
assessment requirement.

## 2. Production mindset

Every implementation decision should account for: idempotency, data consistency,
deterministic execution, environment isolation, failure isolation, bounded
retries, explicit error handling, atomicity where practical, observability,
reproducibility, data-quality enforcement, safe configuration management, CI
validation, and clear ownership between ingestion, transformation, and
orchestration.

Two rules override convenience:

- **Retries must never silently create duplicate business records.**
- **A task either completes successfully or fails visibly.** Never swallow an
  error. No bare `except: pass`, no exit code 0 on a failed load.

## 3. Layer responsibilities

Keep the boundaries clean. Logic belongs to exactly one layer.

**MySQL** — the simulated operational billing source. Bootstrapped from
`seed_data/` by `ingestion/bootstrap_mysql.py`. This bootstrap is source
initialization, not analytics ingestion; truncate-and-load is acceptable there.

**dlt** — owns ingestion from MySQL into the PostgreSQL `raw` schema, and
nothing else. Ingestion must be repeatable and idempotent, and must preserve
source fidelity.

Use `write_disposition="replace"`, never blind append, and declare the business
primary key (`customer_id`, `subscription_id`, `invoice_id`) as a resource hint.
Replace reloads rather than accumulates, so the DAG running every five minutes
and any retry leave the same row counts.

**Do not use `merge` here.** Its dedup step
(`ROW_NUMBER() OVER (PARTITION BY primary_key)`, in `SqlMergeFollowupJob`) would
collapse the planted duplicate rows during ingestion, and the brief requires
raw/staging tests to *catch* those defects. The `primary_key` hint is safe with
replace: dlt maps only the `unique` hint to a Postgres constraint, so no
constraint is emitted and duplicates load intact. Deduplication belongs to dbt
staging, where it is visible, tested, and auditable.

**PostgreSQL `raw`** — preserves source data as closely as practical. Do not fix
business-level quality problems here. Structural coercion needed to load a row is
acceptable; silent cleaning is not. Raw defects are evidence.

**dbt staging** — owns renaming, typing, normalization, basic cleaning, validity
rules, and identification of bad records. This is where trust is established.

**quarantine** — invalid records that must not reach the marts stay observable
and auditable here. Prefer routing invalid rows to a quarantine model over an
unexplained `WHERE`. A reviewer must be able to answer "what happened to the bad
records?" by querying something.

**dbt intermediate** — reusable business logic only where it earns its place. If
logic is used by exactly one mart, it belongs in that mart. Do not create a model
to populate a folder.

**dbt marts** — trusted, consumer-facing business logic for the three required
outputs: MRR by month and plan, customer lifetime value to date, and monthly
churn with MRR lost. Marts consume trusted data only and must pass their tests.

**Airflow** — owns orchestration, dependencies, scheduling, retries, failure
handling, and notification. It must not contain transformation logic that belongs
in dbt, or ingestion logic that belongs in dlt.

## 4. Idempotency

Treat idempotency as a first-class requirement, not a nice-to-have.

Repeated execution of the MySQL bootstrap, dlt ingestion, Airflow retries, and
dbt builds must not duplicate business records or corrupt state.

```
bootstrap once  → 121 customers
bootstrap twice → 121 customers, not 242
dlt run twice   → same row counts in raw, mirroring the source exactly
```

Note that `raw` deliberately mirrors the source *including* its duplicate rows,
so raw row counts are 121 / 175 / 2855 and the business keys are **not** unique
there. Uniqueness is a contract enforced in staging, not an ingestion guarantee.

Before modifying ingestion behavior, state explicitly what happens if the
operation runs twice. If a task can be retried, its write strategy must be safe
to retry.

## 5. Error handling and self-recovery

Distinguish transient from deterministic failures.

**Transient** — temporary database connectivity, network interruption,
short-lived infrastructure failure. These may use **bounded** retries.

**Deterministic** — invalid SQL, schema incompatibility, failed business-rule
tests, missing required columns, broken model contracts. These must fail visibly
and notify. Retrying them wastes time and hides the defect.

Never implement infinite retries.

Never describe masking an error as "self-healing." Self-healing means safely
recovering from a *known transient* failure without human intervention — for
example, a dropped connection retried by Airflow, where the dlt replace load
makes the rerun safe. A dbt test failing on invalid analytical data is **correct
behavior**: do not publish, do notify.

## 6. Atomicity and consistency

Prefer atomic database operations where practical. Avoid workflows that can leave
a partially applied logical update with no clear recovery path. For multi-step
data movement, failures must be detectable and reruns must be safe.

Validate critical invariants after ingestion:

- primary-key uniqueness in each destination table;
- required destination tables exist;
- referential relationships hold (enforced by dbt `relationships` tests);
- source/destination row counts agree for this small static dataset.

Do not build a reconciliation framework. Four assertions in
`ingestion/sync_mysql_to_postgres.py` (or a validation task) are enough.

## 7. Environment isolation

Development behavior must be safe by default.

- `dev` is the default and fallback dbt target.
- Production execution must **explicitly** select `prod`.
- Local DAG tests must never run dbt against production.

```bash
DBT_TARGET=dev    # local, CI, and every fallback
DBT_TARGET=prod   # production Airflow deployment only
```

**`prod` is never a fallback or default value** — not in `profiles.yml`, not in
`dbt_project.yml`, not in the DAG, not in a `os.getenv(..., "prod")` call. The
same DAG code is reused across environments; deployment configuration chooses the
target.

Only one database (`analytics`) exists here, so `dev` and `prod` separate by
**schema**, not by database. Document that.

Credentials stay out of source code. Use environment variables or Airflow
connections. Commit `.env.example`, never `.env`. Never commit secrets.

## 8. dbt execution

Prefer `dbt build` over separate `dbt run` and `dbt test` runs — it interleaves
tests with models and stops bad data from propagating downstream.

The Airflow pipeline runs dbt with the target chosen by the runtime environment.

Materializations reflect the current data volume (~3.1k rows total), not
hypothetical scale:

| Layer | Materialization | Why |
|---|---|---|
| staging | view | lightweight cleanup; no storage duplication justified |
| quarantine | view | small diagnostic sets |
| intermediate | view, or ephemeral when justified | small reusable logic |
| marts | table | stable consumer-facing datasets |

Do not introduce incremental models to demonstrate knowledge of them. Document
incremental strategies as future improvements, with the volume trigger that would
justify the change.

## 9. dbt data quality

Tests are executable data contracts. The brief calls this a first-class part of
the assessment.

Use generic tests where they express a real invariant: `unique`, `not_null`,
`accepted_values`, `relationships`.

Implement at least **two singular tests** encoding billing business rules. Use
columns that exist:

- **Test A** — a paid invoice must never have a non-positive normalized revenue
  amount (catches `I000725`–`I000728`, `I000322`).
- **Test B** — `end_date` must never precede `start_date` (catches `S00034`).

Other valid candidates: MRR is never negative; a cancelled subscription has an
`end_date`; invoice currency is in the supported FX set.

Rules:

- The planted source defects must remain **detectable** — as warnings, stored
  failures, or explicit quarantine models.
- The trusted analytical layer must pass its tests. `dbt build` exits green.
- **Never delete or weaken a meaningful test to make CI green.** Fix the data
  handling upstream instead.
- Records excluded from marts are quarantined or explicitly documented — never
  dropped without a trace.

## 10. Airflow

One DAG: `nordstack_analytics`. Keep it simple and readable.

```
sync_mysql_to_postgres → validate_raw_sync → dbt_build
                                                 ├── success email
                                                 └── failure email
```

Schedule `*/5 * * * *`, as the assignment requires. Email notification on both
success and failure, as the assignment requires.

Production-minded settings:

| Setting | Value | Why |
|---|---|---|
| `catchup` | `False` | no need for historical five-minute backfills |
| `max_active_runs` | `1` | prevents overlapping writes to the same destination |
| `retries` | bounded (e.g. 2) | recovers from transient infrastructure errors |
| `retry_delay` | short | the schedule is five minutes |
| `dagrun_timeout` | under the schedule interval | a stuck run must not silently violate the operating target |

Do not hardcode a production dbt target inside the DAG — that makes local DAG
testing unsafe. Read `DBT_TARGET` from the environment with `dev` as the
fallback. SMTP credentials belong in Airflow connections or environment
configuration, not in the DAG source.

The assignment asks for email only. Do not add Slack or PagerDuty.

## 11. CI/CD

Keep CI component-aware. Do not rebuild every subsystem for every change.

**dbt CI** — on changes under `dbt/`. Run `dbt deps`, `dbt debug`, `dbt build`,
`dbt docs generate` against an isolated CI target/schema.

**Airflow CI** — on changes under `airflow/` or `tests/test_airflow_dag.py`.
Validate Python imports, DAG imports, the expected DAG ID, the schedule,
`catchup=False`, task IDs, and dependency order.

**ingestion CI** — on changes under `ingestion/`. Bootstrap MySQL, run the dlt
sync **twice**, assert row counts are unchanged and business PKs remain unique,
and assert the required destination tables exist. The double-run is the practical
proof of idempotency.

**integration CI** — on changes to shared contracts: source schema, dbt source
definitions, the Airflow commands that invoke dbt, `docker-compose.yml`, or
shared dependency/configuration files. Runs the full chain.

Use GitHub Actions. Trigger on `pull_request` and `push` to `main`.

**Do not fake production CD.** There is no deployment target in this take-home.
Implement meaningful CI and *document* where the CD promotion boundary would sit.

**On Slim CI:** state-aware selection (`state:modified+` with `--defer`) requires
a production `manifest.json` to defer to. No production dbt run exists here, so
there is nothing to defer against. CI runs a full `dbt build` — the dataset is
~3.1k rows and the build is cheap. Slim CI belongs in Next Steps, triggered by a
scheduled production run publishing its manifest as an artifact.

## 12. Change-aware development

Before modifying code, identify the affected subsystem: ingestion, dbt, Airflow,
shared infrastructure, or an integration contract.

Make the smallest coherent change. Do not touch unrelated components unless the
interface contract requires it.

- Changing an upstream schema or ingestion contract → explicitly evaluate
  downstream dbt impact before writing code.
- Changing dbt internals only → do not modify ingestion or Airflow.
- Changing orchestration only → do not rebuild unrelated dbt models unless
  integration behavior actually changed.

## 13. Data governance

Governance here means practical engineering controls, not enterprise bureaucracy.

Maintain: documented grains, model descriptions, column descriptions for
important business fields, explicit assumptions, source-to-mart lineage (dbt
provides it), quality rules, documented handling of invalid data,
configuration/secrets hygiene, and reproducible environments.

`dbt docs generate` must produce useful documentation — the brief requires it.

Do not introduce a standalone data catalog, IAM platform, row-level security
system, or metadata service.

## 14. Observability

Use what the stack already provides: dlt `LoadInfo`, Airflow task logs and
states, dbt build/test results, success/failure emails, CI results.

Do not add Grafana, Prometheus, OpenTelemetry, or similar.

Failures must be visible and actionable. A failure that only appears in a log
nobody reads is not observable.

## 15. Documentation behavior

When an implementation introduces an important architectural assumption,
tradeoff, or production limitation, update `README.md` in the same change.

Document what was implemented, why, the assumptions it rests on, its known
limitations, and what would change at larger scale.

The README must cover, per the brief: setup steps, project structure, key
modeling decisions, the data-quality issues found and how each was handled, and
next steps.

Do not fill documentation with generic production buzzwords. **Every documented
production principle must correspond to implemented behavior, or be clearly
labelled a future improvement.**

## 16. Future-scale decisions

Do not prematurely implement scaling mechanisms. When discussing growth, reason
from concrete factors: row volume, ingestion frequency, model runtime, query
frequency, update patterns, source CDC availability, database cost, concurrency,
SLA requirements.

Deferred to `Next Steps` — mirror this list in the README:

- incremental dbt materializations, with the runtime/volume trigger;
- Slim CI with deferred production state;
- CDC ingestion from MySQL;
- historical/dated FX rate tables;
- PostgreSQL indexing and partitioning;
- a real staging environment and per-PR databases;
- richer observability and freshness alerting;
- production secrets management;
- stronger RBAC;
- infrastructure as code;
- automated quarantine remediation;
- backfill tooling.

## 17. Coding behavior

Before implementing a change:

1. Read the relevant existing code.
2. Read `CANDIDATE_BRIEF.md` and `NORDSTACK_TAKE_HOME_ROADMAP.md` when the change
   affects architecture or requirements.
3. Identify the smallest correct implementation.
4. Consider failure and retry behavior.
5. Consider environment safety.
6. Consider how the change will be tested.
7. Implement.
8. Run the relevant tests/validation.
9. Update documentation when architectural behavior changes.

Do not rewrite working code unnecessarily. Do not add a dependency without a
clear reason (`dbt_utils` and similar are explicitly welcome per the brief).
Prefer explicit, readable code over clever abstractions.

## 18. Decision hierarchy

When uncertain, decide in this order:

1. Correctness
2. Assignment requirements
3. Data safety and consistency
4. Reproducibility
5. Testability
6. Simplicity
7. Operational reliability
8. Performance at the current scale
9. Future scalability

Hypothetical future scale never overrides simplicity for this small dataset.

## 19. Core project principle

> **Build the smallest system that fully satisfies the assignment, while ensuring
> that repeated execution is safe, failures are visible, environments are
> isolated, data-quality rules are enforceable, and each component has a clear
> responsibility.**

When deciding whether to add something, ask: does this help satisfy the
assignment, prove reliability, or make the system easier to review? If no, it
goes in `Next Steps`.

---

## Known divergences

Deliberate departures from the roadmap or from generic best practice, each with
its reason. Do not "fix" these back.

1. **dlt + MySQL is a project choice, not an assignment requirement.** The brief
   marks Step 0 as OPTIONAL and offers `dbt seed` / `COPY` as the alternative
   (Step 1). The roadmap chooses the dlt path because it exercises a real
   source-to-warehouse ingestion boundary. Never claim the assignment mandates it.

2. **CI runs a full `dbt build`, not Slim CI.** No production manifest exists to
   defer against — see §11. Slim CI is a documented Next Step.

3. **Two roadmap test suggestions are unimplementable and must not be written.**
   The roadmap proposes `cancelled_at` and "a paid invoice must have a payment
   timestamp." Neither column exists. Cancellation is `end_date`; there is no
   payment timestamp at all.

4. **`DBT_TARGET` dev/prod isolation (§7) appears in neither the brief nor the
   roadmap.** It is an added engineering control. Because only the `analytics`
   database exists, the targets separate by schema.

5. **Churn MRR-lost uses `raw_subscriptions.monthly_price` directly.** The
   roadmap's fallback to "a deterministic proxy such as the most recent paid
   invoice" is unnecessary — the real recurring price is in the source.

6. **The dbt project lives in `dbt/`,** not the roadmap's illustrative
   `dbt_nordstack/`.
