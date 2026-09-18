-- One row per monthly invoice. The revenue grain for every mart.
-- (CI selector check: this edit should rebuild the invoice lineage, nothing else.)
--
-- Standardizes format only. No deduplication needed -- invoice_id is unique at source.
--
--   D13: I000451's 'PAID ' (uppercase, trailing space) is folded to 'paid' HERE. This
--        is the single most valuable normalization in the project: with the raw value,
--        any `where status = 'paid'` silently drops EUR 29 from revenue, and every
--        consumer would have to remember lower(trim()) forever. Doing it once removes
--        the landmine. The defect stays visible in quarantine, detected from source.
--
-- Left deliberately alone: a null amount (I000322), negative amounts
-- (I000725-I000731), and SEK currency. What to do with those is a business decision
-- the marts make -- exclude, or convert via the FX mapping.

select
    trim(invoice_id)                as invoice_id,
    trim(subscription_id)           as subscription_id,
    invoice_date,
    amount,
    upper(trim(currency))           as currency,
    lower(trim(status))             as status
from {{ source('billing_raw', 'invoices') }}
