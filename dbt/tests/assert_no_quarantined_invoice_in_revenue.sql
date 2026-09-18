-- Mart contract: no quarantined invoice may reach recognised revenue. While this passes,
-- excluded_from_marts is genuinely enforced rather than merely computed.
-- Returns rows only on failure.

select
    r.invoice_id,
    r.subscription_id,
    r.amount_eur
from {{ ref('int_paid_invoices_eur') }} as r
inner join {{ ref('invalid_invoices') }} as q
    on r.invoice_id = q.invoice_id
where q.excluded_from_marts
