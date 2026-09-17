# DISCOVERY.md — Source data inspection (Phase 0)

Closes **Phase 0** of `NORDSTACK_TAKE_HOME_ROADMAP.md` (§4): inspect the supplied data
*before* writing models, and record the grain, primary key, and every planted quality
issue with an explicit handling decision.

The decision table in [§4](#4-decision-table) is the build contract for the staging,
quarantine, and test work in Phases 5–6, and the source for the README's required
"data-quality issues found and how each was handled" section.

Every finding below was verified against `seed_data/` on 2026-09-16. No file was modified.

---

## 1. How to reproduce

Findings come from reading the three CSVs with Python's stdlib `csv` module — no database
and no dependencies, so this is re-runnable from a clean clone:

```python
import csv
def load(p):
    with open(p, newline='') as f:
        return list(csv.DictReader(f))

C = load('seed_data/raw_customers.csv')
S = load('seed_data/raw_subscriptions.csv')
I = load('seed_data/raw_invoices.csv')
```

Checks performed per table: row count vs distinct-key count; blank and untrimmed values per
column; observed value domains; date parseability (`datetime.date.fromisoformat`); foreign-key
resolution; chronological ordering; status/date consistency; numeric sign and nullity; billing
cadence; and cross-table amount agreement.

Values are compared as **raw bytes** (`repr()`), not normalized — otherwise casing and
whitespace defects disappear before they are counted.

---

## 2. Table profiles

### 2.1 `raw_customers`

**Grain:** one row per customer. **Primary key:** `customer_id`.
**121 rows, 120 distinct keys** — the row count is inflated by one duplicate.

| Column | Type | Blank | Untrimmed | Distinct |
|---|---|---:|---:|---:|
| `customer_id` | string `C0001`–`C0120` | 0 | 0 | 120 |
| `customer_name` | string | 0 | 0 | 107 |
| `email` | string | 0 | 0 | 120 |
| `country` | ISO-2 string | 1 | 0 | 11 |
| `created_at` | date `YYYY-MM-DD` | 0 | 0 | 105 |

Countries: `DE` 22, `NL` 16, `FR` 12, `ES` 12, `SE` 11, `IT` 11, `NO` 11, `PT` 10, `FI` 8,
`PL` 7, blank 1. Range of `created_at`: 2024-01-01 → 2027-03-15.

Repeated `customer_name` values (107 distinct of 121) are coincidental namesakes, not
duplicates — the 120 emails and 120 IDs are otherwise unique. Not a defect.

### 2.2 `raw_subscriptions`

**Grain:** one row per subscription. **Primary key:** `subscription_id`.
**175 rows, 174 distinct keys.** A customer may hold several: 79 customers hold 1, 30 hold 2,
12 hold 3.

| Column | Type | Blank | Untrimmed | Distinct |
|---|---|---:|---:|---:|
| `subscription_id` | string `S00001`–`S00174` | 0 | 0 | 174 |
| `customer_id` | FK → customers | 0 | 0 | 121 |
| `plan_name` | enum | 0 | 0 | 3 |
| `monthly_price` | decimal | 0 | 0 | 4 |
| `start_date` | date | 0 | 0 | 157 |
| `end_date` | date, nullable | 123 | 0 | 52 |
| `status` | enum | 0 | 0 | 4 |

Plans: `growth` 62, `starter` 59, `scale` 54. Statuses: `active` 101, `cancelled` 52,
`paused` 21, `ACTIVE` 1.

**The plan → price mapping is perfectly consistent**, with exactly one exception:

| Plan | Monthly price | Rows |
|---|---|---:|
| `starter` | 29.0 | 58 (+1 at `-99.00`) |
| `growth` | 99.0 | 62 |
| `scale` | 299.0 | 54 |

`end_date` is populated **iff** `status = cancelled` — all 52 cancelled rows have one, and no
`active` or `paused` row does. That consistency is itself useful: it means `status` and
`end_date` never contradict each other, so neither needs to be reconciled against the other.

### 2.3 `raw_invoices`

**Grain:** one row per monthly invoice. **Primary key:** `invoice_id`.
**2 855 rows, 2 855 distinct keys** — no duplicates here.

| Column | Type | Blank | Untrimmed | Distinct |
|---|---|---:|---:|---:|
| `invoice_id` | string `I000001`–`I002855` | 0 | 0 | 2 855 |
| `subscription_id` | FK → subscriptions | 0 | 0 | 174 |
| `invoice_date` | date | 0 | 0 | 782 |
| `amount` | decimal | 1 | 0 | 5 |
| `currency` | ISO-3 string | 0 | 0 | 2 |
| `status` | enum | 0 | 1 | 4 |

Statuses: `paid` 2 451, `failed` 204, `open` 199, `PAID ` 1. Currency: `EUR` 2 853, `SEK` 2.
Range of `invoice_date`: 2024-01-08 → 2026-07-28.

**Invoice `amount` always equals the parent subscription's `monthly_price`** — zero mismatches
across all 2 855 rows. Billing cadence is monthly (gaps of 28–31 days), with one exception
noted below.

---

## 3. Issue inventory

14 defects, affecting **4 customers, 5 subscriptions, and 12 invoice rows**. Each is cited by
ID so it can be re-checked.

### 3.1 Customers

**D1 — Duplicate primary key: `C0023`.**
Two rows share `customer_id = C0023`. The rows are **byte-identical** (`Sofia Santos`,
`sofia.santos23@example.com`, `ES`, `2024-08-05`) — there are no conflicting attribute values
to reconcile, so deduplication is lossless. This also explains the single duplicated email
address in the file.

**D2 — Blank `country`: `C0008`.**
`Mateo Meyer` has an empty country string. LTV is required to report country, so this row
surfaces as `UNKNOWN` rather than silently grouping with a real country.

**D3 — Malformed `email`: `C0016`.**
Literal value `not-an-email`. Matches no `local@domain.tld` pattern. `email` feeds none of the
three required marts.

**D4 — Future `created_at`, inconsistent with its own subscription: `C0041`.**
Created `2027-03-15` — roughly six months in the future, and nearly three years *after* its own
subscription `S00054` started on `2024-06-24`. A customer cannot be created after they
subscribe. Flags both a future-date and a cross-table chronology violation.

### 3.2 Subscriptions

**D5 — Duplicate primary key: `S00006`.**
Two byte-identical rows (`C0006`, `starter`, `29.0`, start `2025-05-23`, `active`). Lossless to
deduplicate, same as D1.

**D6 — Orphan foreign key: `S00011` → `C9999`.**
References a customer that does not exist. The subscription is `scale` / €299, `cancelled`
2025-10-08, and it carries invoices — including one of the two SEK invoices (D14). Left
unhandled it would produce a customer-less row in LTV and a phantom cancellation in churn.

**D7 — Status casing variant: `S00026`.**
`status = 'ACTIVE'` where every other row uses lowercase. Purely a normalization issue; the row
is otherwise sound.

**D8 — `end_date` precedes `start_date`, and billing continued: `S00034`.**
`start_date` 2025-03-29, `end_date` 2025-03-19 — cancelled ten days *before* it began. The
billing history contradicts the cancellation outright: **17 invoices run from 2025-03-29 to
2026-07-28**, 13 of them `paid`, totalling **€3,887** on the `scale` plan. This is the only
subscription in the file with invoices dated after its `end_date`.

The `end_date` is therefore the untrustworthy field, not the invoices. See
[§5.1](#51-s00034--corrupt-cancellation-date-with-16-months-of-subsequent-billing).

**D9 — Negative `monthly_price`: `S00048`.**
`-99.00` on a `starter` plan, which is priced at `29.0` everywhere else — so the value is wrong
in both sign *and* magnitude. Not an isolated cell: it propagates into seven invoices (D12).

### 3.3 Invoices

**D10 — Orphan foreign key: `I000601` → `S99999`.**
References a subscription that does not exist. Status `paid`, amount €299. Because it cannot be
joined to a subscription, it has no plan and no customer — it would inflate total MRR while
being unattributable in any breakdown.

**D11 — `paid` invoice with blank `amount`: `I000322`.**
Status `paid`, `amount` empty, dated 2026-02-06. Its subscription `S00019` is `growth` at €99,
so a value *could* be inferred — see [§5.3](#53-i000322--paid-invoice-with-no-amount).

**D12 — Negative amounts: `I000725`–`I000731` (7 rows).**
All belong to `S00048` (D9) and inherit its `-99.00` price: five `paid`, two `failed`, spanning
2024-01-23 to 2024-07-23. Consistent with the negative price being a source-system data-entry
defect that then flowed into generated invoices.

**D13 — Status casing and trailing whitespace: `I000451`.**
`status = 'PAID '` — both uppercase and trailing-space. An exact-match filter on `'paid'` would
silently drop this invoice's €29 from MRR, which is precisely why normalization must precede
filtering.

**D14 — Non-EUR currency: `I000101`, `I000201`.**
Both denominated in `SEK` at `299.00` against `scale` subscriptions. `I000101` is `paid`;
`I000201` is `open` (and belongs to the orphan `S00011`). See
[§5.2](#52-sek-invoices--genuine-currency-or-mislabelled-amount).

### 3.4 Edge cases — real, but not defects

Recorded so they are not later mistaken for bugs.

**E15 — Future-dated subscription with no invoices: `S00149`.**
Starts 2026-10-05, `active`, `scale`, zero invoices — the only subscription in the file with no
billing history. Legitimate: it has not started yet, and the billing system issues invoices only
once a subscription takes effect. "Zero invoices" here means "not yet billed", not "billing lost".

Recorded because it breaks careless SQL. Its customer `C0101` holds three subscriptions:

| Subscription | Plan | Start | Status | Invoices | Paid |
|---|---|---|---|---:|---:|
| `S00147` | starter | 2025-01-19 | paused | 19 | €464 |
| `S00148` | scale | 2025-11-21 | paused | 9 | €2,691 |
| `S00149` | scale | 2026-10-05 | **active** | 0 | €0 |

Three consequences the marts must handle:

- **LTV** — an `INNER JOIN` from subscriptions to invoices drops `S00149` entirely. The customer
  survives here (the other two carry revenue), but the *plan mix* would be wrong: `C0101` holds
  two `scale` subscriptions and the join would report one.
- **Current status** — the §5.4 rule is "active if any subscription is active". `S00149` is the
  only active one; the others are `paused`. So `C0101` reports as active because of a
  subscription that has not started. Defensible, but a decision rather than an accident.
- **MRR** — must contribute €0, producing neither an empty row nor a `NULL` month.

No test and no quarantine entry: there is nothing to repair. What it needs is marts that do not
choke on it, which is why §4 assigns it mart-level tests instead of a source test.

**E16 — Cancellations dated in the future: `S00020` (2027-03-15), `S00139` (2027-01-26),
`S00167` (2027-01-02).**
Scheduled cancellations that have not taken effect yet. Normal SaaS behaviour, but it means the
churn mart will contain future months. Documented as an assumption rather than filtered.

**E17 — One missed billing month: `S00040`.**
A 61-day gap between 2024-11-22 and 2025-01-22 — December 2024 was never invoiced. The
subscription is `active` throughout. Every other consecutive gap in the file is 28–31 days.
Represents a real-world billing miss; no metric is broken by it.

### 3.5 Verified absent

Stating what is *not* wrong bounds the test surface — these need no tests, quarantine, or
defensive SQL:

- **No unparseable dates** in any date column of any table.
- **No double-billing** — `(subscription_id, invoice_date)` is unique across all 2 855 invoices.
- **No invoice dated before its subscription's `start_date`.**
- **No invoice whose `amount` disagrees with its subscription's `monthly_price`.**
- **No customer without a subscription.**
- **No future-dated invoices.**
- **No status/`end_date` contradiction** — `end_date` is populated exactly when
  `status = cancelled`.
- **No gaps in any ID sequence** — the three keys run `1..120`, `1..174` and `1..2855` with
  every integer present and consistent zero-padding. Nothing was lost between the billing
  system and the CSV export, so every anomaly below is a *planted* defect rather than a
  possible export artifact. It also confirms the duplicates (D1, D5) reuse an existing ID
  rather than being extra rows with fresh ones, and that the orphan FK targets `C9999` and
  `S99999` are synthetic sentinels far outside the real range — not off-by-one boundary bugs.

### 3.6 Headline impact

| Measure | Value |
|---|---:|
| Invoices total | 2 855 |
| Invoices `paid` (after normalizing `'PAID '`) | 2 452 |
| Paid invoices with an unusable amount (D11, D12) | 6 |
| Paid invoices not in EUR (D14) | 1 |
| Cancelled subscriptions | 52 |
| Clean paid EUR revenue | €325 555 |

Defects touch **4 of 121** customer rows, **5 of 175** subscription rows, and **12 of 2 855**
invoice rows — small enough that quarantining is cheap, large enough that ignoring them would
visibly distort every mart.

---

## 4. Decision table

The contract for Phases 5–6. "Detectable at" matters because one defect stops being visible
after ingestion — see [§6](#6-implications-for-earlier-phases).

| # | Issue | Detectable at | Handling | Test that proves it |
|---|---|---|---|---|
| D1 | Duplicate PK `C0023` | raw / staging | Deduplicate on `customer_id` **in staging** (rows identical → lossless). Ingestion preserves it; see §6 | `unique` + `not_null` on `stg_customers.customer_id` |
| D2 | Blank `country` `C0008` | raw / staging | Normalize to `'UNKNOWN'`; keep the row — country is reported by LTV but does not invalidate revenue | `not_null` on `stg_customers.country` (passes post-normalization) |
| D3 | Malformed `email` `C0016` | raw / staging | Flag `is_valid_email = false`; keep the row. Feeds no required mart | warn-level singular test on the raw layer; no error-level test |
| D4 | Future `created_at` `C0041` | raw / staging | Flag `is_future_created = true`; keep the row. Do not clamp or drop | warn-level test: `created_at <= current_date` |
| D5 | Duplicate PK `S00006` | raw / staging | Deduplicate on `subscription_id` **in staging** (rows identical → lossless) | `unique` + `not_null` on `stg_subscriptions.subscription_id` |
| D6 | Orphan FK `S00011` → `C9999` | raw / staging | **Quarantine** → `invalid_subscriptions`, reason `orphan_customer`. Excluded from all marts | `relationships` subscriptions → customers on the accepted set |
| D7 | `'ACTIVE'` casing `S00026` | raw / staging | Normalize: `lower(trim(status))` | `accepted_values` on `stg_subscriptions.status` = `active, cancelled, paused` |
| D8 | `end_date` < `start_date` `S00034` | raw / staging | Set `end_date` to `NULL`, flag to quarantine, **exclude from the churn mart only**. Its 13 paid invoices stay in MRR and LTV | **Singular test B** on the accepted set; quarantine row is the audit trail |
| D9 | Negative `monthly_price` `S00048` | raw / staging | **Quarantine** → `invalid_subscriptions`, reason `non_positive_price` | singular test: `monthly_price > 0` on the accepted set |
| D10 | Orphan FK `I000601` → `S99999` | raw / staging | **Quarantine** → `invalid_invoices`, reason `orphan_subscription`. €299 excluded from MRR | `relationships` invoices → subscriptions on the accepted set |
| D11 | `paid` with blank `amount` `I000322` | raw / staging | **Quarantine** → `invalid_invoices`, reason `paid_with_null_amount`. Do not impute | **Singular test A** on the accepted set |
| D12 | Negative amounts `I000725`–`I000731` | raw / staging | **Quarantine** → `invalid_invoices`, reason `non_positive_amount` (all 7, paid and failed alike) | **Singular test A** on the accepted set |
| D13 | `'PAID '` casing `I000451` | raw / staging | Normalize: `lower(trim(status))` — **before** any `= 'paid'` filter | `accepted_values` on `stg_invoices.status` = `paid, open, failed` |
| D14 | SEK invoices `I000101`, `I000201` | raw / staging | Treat as genuine SEK; convert via a static documented FX map | `accepted_values` on `currency` = `EUR, SEK`; singular test: every currency resolves to a rate |
| E15 | Future start, no invoices `S00149` | staging / marts | Keep. Must contribute €0 to MRR and appear in LTV with zero revenue | mart test: LTV `total_revenue_eur >= 0`, `not_null` |
| E16 | Future `end_date` ×3 | staging / marts | Keep. Churn mart legitimately contains future months | documented assumption; no test |
| E17 | Missed billing month `S00040` | observation only | No action | none |

**Quarantine is a model, not a `WHERE` clause.** Every row removed by D6, D8, D9, D10, D11 or
D12 lands in `invalid_customers` / `invalid_subscriptions` / `invalid_invoices` carrying its
rejection reason, so "what happened to the bad records?" is answerable with a query
(`CLAUDE.md` §3).

---

## 4a. Test coverage — reconciling 17 items against 12 warnings

A frequent question when reading a `dbt build`: this document lists **17 items**, but only
**12 warnings** fire. Nothing is missing. The arithmetic:

**17 items = 14 defects + 3 edge cases.**
**13 of the 14 defects are tested, in 12 warnings**, because one singular test catches two
defects at once.

| # | Item | Test | Fires |
|---|---|---|---|
| D1 | duplicate `C0023` | `unique` on the source | ✅ |
| D2 | blank `country` `C0008` | `assert_customer_country_is_present` | ✅ |
| D3 | malformed email `C0016` | `assert_customer_email_is_valid` | ✅ |
| D4 | future `created_at` `C0041` | `assert_customer_created_at_not_future` | ✅ |
| D5 | duplicate `S00006` | `unique` on the source | ✅ |
| D6 | orphan `S00011` → `C9999` | `relationships` | ✅ |
| D7 | `'ACTIVE'` casing `S00026` | `accepted_values` | ✅ |
| D8 | `end_date` < `start_date` `S00034` | **singular B** | ✅ |
| D9 | negative price `S00048` | `assert_subscription_price_is_positive` | ✅ |
| D10 | orphan `I000601` → `S99999` | `relationships` | ✅ |
| D11 | `paid` with null amount `I000322` | **singular A** | ✅ shared |
| D12 | negative amounts `I000725`–`I000731` | **singular A** | ✅ shared |
| D13 | `'PAID '` casing `I000451` | `accepted_values` | ✅ |
| D14 | SEK invoices | *(none — by design)* | ➖ |
| E15 | `S00149` future start, no invoices | *(none — not a defect)* | ➖ |
| E16 | 3 future cancellations | *(none — not a defect)* | ➖ |
| E17 | `S00040` missed billing month | *(none — no action)* | ➖ |

**Singular test A reports `WARN 6`** — `I000322` plus the five *paid* invoices among
`I000725`–`I000731`. One test, two defects, six rows. That is why 13 tested defects produce
12 warnings.

### Why four items have no test

- **D14 (SEK)** — §5.2 decided SEK is a legitimate currency, converted through the FX mapping.
  Warning about it would be alerting on correct data. `accepted_values` on `currency` lists
  `EUR, SEK` precisely so a *third* currency would fire.
- **E15, E16, E17** — normal SaaS behaviour, not defects. A subscription that starts in 18 days
  has no invoices yet; a cancellation scheduled for next year has not happened yet; one missed
  billing month is an operational fact. None is repairable, so none is testable. What they need
  is marts that handle them without breaking — see the mart-level tests in §4.

### Where tests live

```
raw    -> 20 tests, severity WARN     defects must be visible, never block the build
staging -> none                        standardizes format, asserts nothing
marts  -> severity ERROR               the published contract; bad data must stop here
```

Staging carries no tests by design. It sits between the two ends of the pipeline: testing it
would mostly restate the raw tests against a copy of the same rows, and the defects it does not
repair — orphans, negative amounts, the impossible `end_date` — would fail there by design.
Detection belongs to raw and quarantine; enforcement belongs to the marts.

---

## 5. Ambiguities and assumptions

Three defects had no single defensible reading. Each was decided explicitly.

### 5.1 `S00034` — corrupt cancellation date with 16 months of subsequent billing

**The conflict.** `end_date` (2025-03-19) precedes `start_date` (2025-03-29), yet 17 invoices
were issued afterwards, 13 of them paid, through 2026-07-28. Exactly one of the two facts can
be true.

**Decision.** Trust the billing history, distrust the date. Null the `end_date`, flag the row to
quarantine, and exclude it from **the churn mart only**. Its 13 paid invoices (€3,887) remain in
MRR and LTV.

**Why.** Sixteen months of subsequent invoices is far stronger evidence than a single date
field that is internally impossible. Quarantining the whole subscription would discard €3,887 of
genuine paid revenue and orphan 17 invoices; trusting the `end_date` would publish a
cancellation that the same dataset contradicts. Excluding only the churn contribution removes
the one metric that cannot be computed honestly, and costs nothing else.

**Cost.** Churn for 2025-03 omits one cancellation. Documented rather than silently absorbed.

### 5.2 SEK invoices — genuine currency, or mislabelled amount?

**The conflict.** Both SEK invoices carry `amount = 299.00` against `scale` subscriptions priced
at €299 — suspiciously identical. Either the customer is billed 299 SEK (≈ €26), or a EUR amount
was tagged with the wrong currency.

**Decision.** Treat as genuine SEK and convert through a static FX mapping:

```
EUR -> 1.00
SEK -> 0.087   # static rate, assessment only
```

`I000101` (paid) contributes **€26.01** to MRR instead of €299. `I000201` is `open`, so it never
reaches a revenue metric regardless.

**Why.** The brief explicitly asks for a single reporting currency with a stated FX assumption;
treating the field as real is the reading that honours the `currency` column and exercises the
requirement. The revenue at stake is ~€26 of €325 555, so the choice is immaterial to every
headline number — which is exactly why the more honest reading is affordable.

**Reporting currency: EUR**, since NordStack operates across Europe and 2 853 of 2 855 invoices
are already EUR. Dated historical FX rates → Next Steps.

### 5.3 `I000322` — `paid` invoice with no amount

**The conflict.** The amount is recoverable: the parent subscription `S00019` is `growth` at €99,
and every other invoice in the file matches its subscription's price exactly.

**Decision.** Quarantine. Do not impute.

**Why.** "Every other invoice matches its plan price" is a pattern, not a guarantee — and
inventing €99 of revenue the source never asserted is precisely the silent cleaning that
`CLAUDE.md` §3 forbids. €99 of €325 555 is not worth manufacturing. The quarantine row keeps the
defect visible and reversible; imputation would hide it permanently.

### 5.4 Standing assumptions

- **`current subscription status` for LTV** — a customer may hold up to 3 subscriptions.
  Deterministic rule: `active` if any subscription is active; otherwise the status of the
  most recently started subscription. Ties broken by `subscription_id`.
- **MRR is derived from paid invoices**, per the brief's explicit instruction — not from
  subscription contract value.
- **Churn month = the month of `end_date`**; MRR lost = the subscription's `monthly_price`,
  which is present in the source and needs no proxy.
- **`paid` is the only revenue-recognized status.** `open` and `failed` contribute nothing.

---

## 6. Implications for earlier phases

The duplicate-key defect (D1, D5) forces an ingestion decision, because the obvious
configuration destroys it.

### The conflict

Roadmap §7.2 prescribes `write_disposition="merge"` on the business primary key, for
idempotency. But dlt's merge job deduplicates — `ROW_NUMBER() OVER (PARTITION BY primary_key)`
in `SqlMergeFollowupJob` — so D1 and D5 would collapse *during ingestion*, and Postgres `raw`
would land 120 customers and 174 subscriptions instead of 121 and 175.

`CANDIDATE_BRIEF.md` §3 is explicit: *"The raw data contains deliberately planted quality
issues. Your tests on the raw/staging layer should **catch** them."* A defect that no longer
exists cannot be caught. The brief outranks the roadmap, so ingestion must preserve source
fidelity.

### The resolution

**`write_disposition="replace"`, with the business primary key declared as a resource hint.**
Verified against dlt 1.30.0:

- Dedup lives **only** in the merge path; `SqlStagingReplaceFollowupJob` never calls it, so
  duplicates load intact.
- The `primary_key` hint yields `{'nullable': False, 'primary_key': True}` — dlt maps only the
  `unique` hint to a Postgres constraint (`HINT_TO_POSTGRES_ATTR = {"unique": "UNIQUE"}`), so
  **no constraint is emitted** and the duplicate rows are accepted.
- Replace reloads rather than accumulates, so re-running is idempotent: counts stay
  121 / 175 / 2855 and never double.

All three properties at once: defects preserved, business key documented, reruns safe.

### Consequences

1. **D1 and D5 are detectable in `raw` and `staging`**, by the ordinary dbt `unique` tests —
   exactly where the brief expects them. Deduplication moves to staging, where it is visible,
   tested, and auditable rather than an invisible side effect of ingestion.

2. **The MySQL bootstrap must not declare a `PRIMARY KEY`** on `customers` or `subscriptions`.
   With one, the duplicate row fails to insert and the defect is destroyed before dlt ever runs
   — the bootstrap would be "fixing" the data it exists to reproduce. The same reasoning
   removes `FOREIGN KEY`, `CHECK`, and `NOT NULL` on the affected columns; see
   `ingestion/ddl/mysql_source.sql`, where each omission is commented with the defect it
   protects.

3. **The roadmap's consistency check holds exactly.** Roadmap §19 proposes source `COUNT(*)` =
   destination `COUNT(*)`; under replace that is correct as written —
   **121 / 175 / 2855 on both sides** — with no distinct-key adjustment needed.

**Cost of this choice:** a full reload every run instead of an incremental merge. At 3 151 rows
that is free. Revisit when volume, source CDC availability, or run frequency make full reloads
expensive — at which point `merge` returns and duplicate detection moves to the ingestion
validation step. → Next Steps.

---

## 7. Test inventory

What §4 produces, ready to implement in Phase 5.

### Generic tests

| Test | Applied to |
|---|---|
| `unique`, `not_null` | `customer_id`, `subscription_id`, `invoice_id` |
| `relationships` | `stg_subscriptions.customer_id` → `stg_customers`; `stg_invoices.subscription_id` → `stg_subscriptions` |
| `accepted_values` | `plan_name` (`starter/growth/scale`); subscription `status` (`active/cancelled/paused`); invoice `status` (`paid/open/failed`); `currency` (`EUR/SEK`) |
| `not_null` | `stg_customers.country` (post-`UNKNOWN` normalization) |

### Singular tests (the brief requires ≥ 2)

**Test A — paid revenue must be economically valid.**
No invoice with normalized `status = 'paid'` may have a null or non-positive amount in EUR.
Catches D11 and D12 (6 paid rows). Runs against the accepted set, where it passes.

**Test B — subscription dates must be chronologically valid.**
`end_date` must never precede `start_date`. Catches D8. Runs against the accepted set, where it
passes because D8's `end_date` has been nulled.

### Supporting tests

- `monthly_price > 0` on accepted subscriptions (D9).
- Every `currency` present resolves to an FX rate (D14) — fails loudly if a new currency appears.
- Mart-level: MRR is never negative; LTV `total_revenue_eur >= 0`; churn `mrr_lost >= 0`.

### Severity split

Per `CLAUDE.md` §9, the same rules run at two severities: **warn / store-failures** on the raw
layer, so the planted defects stay visible and countable; **error** on staging and marts, so
`dbt build` fails if bad data ever reaches the trusted layer. The final `dbt build` exits green
because the defects are quarantined upstream — not because the tests were weakened.

---

## 8. Phase 0 "Done when" checklist

Roadmap §4 requires all four to be answerable.

| Requirement | Status |
|---|---|
| The grain of every raw table | ✅ [§2](#2-table-profiles) — customer, subscription, monthly invoice |
| The primary key of every table | ✅ `customer_id`, `subscription_id`, `invoice_id` — all verified, two with duplicates |
| Every planted quality issue found | ✅ [§3](#3-issue-inventory) — 14 defects + 3 edge cases, cited by ID, plus 7 conditions verified absent |
| How each issue will be handled | ✅ [§4](#4-decision-table) — layer, handling, and proving test for each |

**Phase 0 is complete.** Phase 1 (Docker: MySQL + Postgres) and Phase 2 (source DDL + bootstrap)
have since been built on it — every defect in §3 was verified to survive the load into MySQL,
and [§6](#6-implications-for-earlier-phases) determined both the constraint-free DDL and the
`replace` ingestion strategy. Next: Phase 3, the dlt sync into Postgres `raw`.
