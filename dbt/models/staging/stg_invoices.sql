-- One row per invoice. Status is normalized here, so a downstream filter on 'paid'
-- also catches I000451's 'PAID '.

select
    trim(invoice_id)                as invoice_id,
    trim(subscription_id)           as subscription_id,
    invoice_date,
    amount,
    upper(trim(currency))           as currency,
    lower(trim(status))             as status
from {{ source('billing_raw', 'invoices') }}
