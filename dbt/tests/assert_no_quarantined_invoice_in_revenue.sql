-- MART CONTRACT -- no quarantined invoice may reach recognised revenue.
--
-- Error severity (the default), unlike the raw tests: this is the boundary the whole
-- quarantine design exists to protect. If an excluded invoice appears in the revenue
-- base, the marts are publishing money the source could not substantiate -- an orphan
-- with no customer to attribute it to, or a paid invoice with no amount.
--
-- This is the test that would have caught the failure mode we designed around: as long
-- as it passes, `excluded_from_marts` is genuinely enforced rather than merely computed.
--
-- Returns rows only on failure.

select
    r.invoice_id,
    r.subscription_id,
    r.amount_eur
from {{ ref('int_paid_invoices_eur') }} as r
inner join {{ ref('invalid_invoices') }} as q
    on r.invoice_id = q.invoice_id
where q.excluded_from_marts
