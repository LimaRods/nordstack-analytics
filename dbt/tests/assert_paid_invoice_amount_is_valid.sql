{{ config(severity = 'warn') }}

-- Paid revenue must be economically valid. Runs against raw at warn severity, so the
-- defects are reported as they arrive. status is normalized inline because raw has not
-- been cleaned yet -- an exact match on 'paid' would miss I000451's 'PAID '.
-- Expected: 6 rows -- I000322 and the five paid invoices priced at -99.00.

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
