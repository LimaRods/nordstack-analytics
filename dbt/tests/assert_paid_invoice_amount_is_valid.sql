{{ config(severity = 'warn') }}

-- Paid revenue must be economically valid. status is normalized inline because raw
-- has not been cleaned yet.

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
