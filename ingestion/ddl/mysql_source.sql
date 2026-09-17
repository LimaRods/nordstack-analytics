-- NordStack — simulated operational billing source (MySQL 8.0)
--
-- Executed by ingestion/bootstrap_mysql.py. Represents the billing system that
-- produced seed_data/*.csv. This is SOURCE INITIALIZATION, not analytics ingestion:
-- dlt reads these tables in Phase 3 and lands them in the PostgreSQL `raw` schema.
--
-- ---------------------------------------------------------------------------
-- DELIBERATE OMISSION: no PRIMARY KEY, FOREIGN KEY, CHECK or NOT NULL constraints
-- ---------------------------------------------------------------------------
-- A real billing system would declare all of them. They are omitted here because
-- the assessment ships deliberately planted defects that constraints would reject
-- at INSERT time, destroying the very data-quality issues the dbt tests must catch
-- (CANDIDATE_BRIEF.md section 3). See DISCOVERY.md for the full inventory.
--
--   PRIMARY KEY  -> would reject duplicate rows C0023 (customers), S00006 (subscriptions)
--   FOREIGN KEY  -> would reject orphans S00011 -> C9999, I000601 -> S99999
--   CHECK        -> would reject negative money: S00048, I000725-I000731
--   NOT NULL     -> would reject blank country (C0008) and null amount (I000322)
--
-- The business key is declared instead where it does not destroy evidence:
-- as a primary_key hint on the dlt resource, and as dbt `unique` + `not_null`
-- tests in staging. Enforcement lives in the trust layer, not in the raw source.
--
-- Type notes:
--   DECIMAL(10,2) for money  -- never FLOAT; these values sum into every revenue metric
--   DATE for dates           -- all values in seed_data parse cleanly (DISCOVERY.md 3.5)
--   VARCHAR for status/currency/country -- preserves 'PAID ' and 'ACTIVE' byte-for-byte

DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id     VARCHAR(20),
    customer_name   VARCHAR(255),
    email           VARCHAR(255),
    country         VARCHAR(10),     -- nullable: C0008 has no country
    created_at      DATE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS subscriptions;
CREATE TABLE subscriptions (
    subscription_id VARCHAR(20),
    customer_id     VARCHAR(20),     -- no FK: S00011 references the non-existent C9999
    plan_name       VARCHAR(50),
    monthly_price   DECIMAL(10,2),   -- no CHECK: S00048 carries -99.00
    start_date      DATE,
    end_date        DATE,            -- nullable: populated only when status = cancelled
    status          VARCHAR(50)      -- raw casing preserved: 'ACTIVE' on S00026
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS invoices;
CREATE TABLE invoices (
    invoice_id      VARCHAR(20),
    subscription_id VARCHAR(20),     -- no FK: I000601 references the non-existent S99999
    invoice_date    DATE,
    amount          DECIMAL(10,2),   -- nullable: I000322 is paid with no amount
    currency        VARCHAR(10),     -- EUR, plus two SEK invoices
    status          VARCHAR(50)      -- raw casing/whitespace preserved: 'PAID ' on I000451
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
