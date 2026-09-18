-- One row per monthly invoice. The revenue grain for every mart.
-- No deduplication needed -- invoice_id is unique at source.
--
-- Normalizing status here is the most valuable cleanup in the project: I000451 holds
-- 'PAID ' with a trailing space, so any `where status = 'paid'` would silently drop it.
-- Amounts and currency are left alone -- what to do with a null amount, a negative one,
-- or SEK is a business decision the marts make.

select
    trim(invoice_id)                as invoice_id,
    trim(subscription_id)           as subscription_id,
    invoice_date,
    amount,
    upper(trim(currency))           as currency,
    lower(trim(status))             as status
from {{ source('billing_raw', 'invoices') }}
