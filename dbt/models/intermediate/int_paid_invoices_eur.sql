-- Paid invoices, normalized to EUR. The revenue base for both fct_mrr and customer_ltv.
--
-- Three things happen here, and each is a business decision rather than formatting:
--   1. Only `paid` counts as revenue. `open` and `failed` contribute nothing.
--   2. Quarantined invoices are dropped -- unattributable, or with an unusable amount.
--   3. Amounts are converted to EUR through int_fx_rates.
--
-- The status filter is safe because stg_invoices already normalized casing: I000451's
-- 'PAID ' is folded to 'paid' upstream, so its EUR 29 is included rather than silently
-- dropped by an exact match.
--
-- Joined to int_subscriptions (not stg_subscriptions) so an invoice belonging to an
-- excluded subscription cannot slip into revenue through the back door.

with invoices as (

    select * from {{ ref('stg_invoices') }}
    where status = 'paid'

),

excluded as (

    select invoice_id
    from {{ ref('invalid_invoices') }}
    where excluded_from_marts

)

select
    i.invoice_id,
    i.subscription_id,
    s.customer_id,
    s.plan_name,
    i.invoice_date,
    date_trunc('month', i.invoice_date)::date  as invoice_month,
    i.amount,
    i.currency,
    round(i.amount * fx.rate_to_eur, 2)        as amount_eur

from invoices as i
inner join {{ ref('int_subscriptions') }} as s
    on i.subscription_id = s.subscription_id
inner join {{ ref('int_fx_rates') }} as fx
    on i.currency = fx.currency
where i.invoice_id not in (select invoice_id from excluded)
