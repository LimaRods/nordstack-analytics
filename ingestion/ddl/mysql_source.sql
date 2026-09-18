-- NordStack -- simulated operational billing source (MySQL 8.0).
-- Executed by ingestion/bootstrap_mysql.py; dlt reads these tables into PostgreSQL raw.
--
-- No PRIMARY KEY, FOREIGN KEY, CHECK or NOT NULL constraints, deliberately. A real
-- billing system would declare all of them; here each would reject a planted defect at
-- INSERT time and destroy the data-quality issue the dbt tests exist to catch:
--
--   PRIMARY KEY  -> duplicate rows C0023, S00006
--   FOREIGN KEY  -> orphans S00011 -> C9999, I000601 -> S99999
--   CHECK        -> negative money S00048, I000725-I000731
--   NOT NULL     -> blank country C0008, null amount I000322
--
-- The business key is declared where it destroys no evidence: as a dlt resource hint,
-- and as dbt tests. Enforcement lives in the trust layer, not in the raw source.
--
-- Money is DECIMAL, never FLOAT: these values sum into every revenue metric.

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
