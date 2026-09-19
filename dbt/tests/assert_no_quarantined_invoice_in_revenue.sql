-- Mart contract: while this passes, excluded_from_marts is enforced, not just computed.

select
    r.invoice_id,
    r.subscription_id,
    r.amount_eur
from {{ ref('int_paid_invoices_eur') }} as r
inner join {{ ref('issues_invoices') }} as q
    on r.invoice_id = q.invoice_id
where q.excluded_from_marts
