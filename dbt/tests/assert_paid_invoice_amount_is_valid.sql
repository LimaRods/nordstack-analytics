{{ config(severity = 'warn') }}

-- SINGULAR TEST A -- paid revenue must be economically valid.
--
-- Runs against RAW, so it reports the defects as they arrive from the billing system.
-- Warn severity: raw is expected to be dirty, and a failure here must not stop
-- `dbt build` from constructing the downstream models. The rows it finds are the ones
-- quarantine/invalid_invoices captures.
--
-- status is normalized inline (lower/trim) because raw has not been cleaned yet:
-- I000451 holds 'PAID ' with a trailing space, and an exact match on 'paid' would
-- miss it. The same normalization is done properly in stg_invoices.
--
-- Expected: 6 rows -- I000322 (paid, null amount, D11) and the five paid invoices
-- among I000725-I000731 (-99.00, D12).

select
    invoice_id,
    subscription_id,
    invoice_date,
    status,
    amount,
    currency
from {{ source('billing_raw', 'invoices') }}
where lower(trim(status)) = 'paid'
  and (amount is null or amount <= 0)
