-- Lifetime value to date. Grain: one row per customer, including those with no revenue.
--
-- LEFT JOINs throughout: an INNER join would silently drop customers whose
-- subscriptions have not billed yet and understate their plan mix. Revenue is
-- COALESCEd to 0 rather than left NULL.
--
-- Current status, since a customer may hold several subscriptions: 'active' if any is
-- active, otherwise the status of the most recently started one. This can report a
-- customer as active on the strength of a subscription that has not begun yet.

with customers as (

    select * from {{ ref('stg_customers') }}

),

subscriptions as (

    select * from {{ ref('int_subscriptions') }}

),

revenue as (

    select
        customer_id,
        sum(amount_eur)     as total_revenue_eur,
        count(*)            as paid_invoices,
        min(invoice_date)   as first_paid_invoice_date,
        max(invoice_date)   as last_paid_invoice_date
    from {{ ref('int_paid_invoices_eur') }}
    group by customer_id

),

subscription_summary as (

    select
        customer_id,
        count(*)                                                  as subscription_count,
        count(*) filter (where status = 'active')                 as active_subscriptions,
        -- Sorted so the value is stable across runs and comparable between customers.
        string_agg(distinct plan_name, ', ' order by plan_name)   as plan_mix
    from subscriptions
    group by customer_id

),

current_status as (

    select distinct on (customer_id)
        customer_id,
        status as current_subscription_status
    from subscriptions
    order by
        customer_id,
        (status = 'active') desc,   -- any active subscription wins
        start_date desc,            -- otherwise the most recently started
        subscription_id             -- deterministic tie-break
)

select
    c.customer_id,
    c.customer_name,
    coalesce(c.country, 'UNKNOWN')              as country,
    c.created_at                                as customer_since,

    coalesce(r.total_revenue_eur, 0)            as total_revenue_eur,
    coalesce(r.paid_invoices, 0)                as paid_invoices,
    r.first_paid_invoice_date,
    r.last_paid_invoice_date,

    coalesce(s.subscription_count, 0)           as subscription_count,
    coalesce(s.active_subscriptions, 0)         as active_subscriptions,
    s.plan_mix,
    cs.current_subscription_status

from customers as c
left join revenue as r              on c.customer_id = r.customer_id
left join subscription_summary as s on c.customer_id = s.customer_id
left join current_status as cs      on c.customer_id = cs.customer_id
