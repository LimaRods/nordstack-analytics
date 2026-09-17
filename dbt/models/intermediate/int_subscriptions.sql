-- Subscriptions the marts are allowed to consume, with churn eligibility resolved.
--
-- Reused by customer_ltv and subscription_churn, which is what earns it a place in
-- intermediate rather than being inlined twice.
--
-- Two filters, deliberately distinct:
--   excluded_from_marts -> the row cannot be attributed or priced at all. Dropped here.
--                          (S00011 orphaned from C9999; S00048 priced at -99.00)
--   unreliable_end_date -> the row is fine, but its cancellation date is not. Kept, and
--                          flagged so only the churn mart excludes it. S00034 billed for
--                          16 months after its supposed cancellation, so its EUR 3,887
--                          belongs in MRR and LTV -- only its churn month is unusable.
--                          See DISCOVERY.md section 5.1.

with subscriptions as (

    select * from {{ ref('stg_subscriptions') }}

),

quarantined as (

    select
        subscription_id,
        excluded_from_marts,
        has_invalid_end_date
    from {{ ref('invalid_subscriptions') }}

)

select
    s.subscription_id,
    s.customer_id,
    s.plan_name,
    s.monthly_price,
    s.start_date,
    s.end_date,
    s.status,

    coalesce(q.has_invalid_end_date, false) as has_unreliable_end_date,

    -- A cancellation counts as churn only if we can trust WHEN it happened.
    (s.status = 'cancelled'
     and s.end_date is not null
     and not coalesce(q.has_invalid_end_date, false)) as is_churn_eligible

from subscriptions as s
left join quarantined as q
    on s.subscription_id = q.subscription_id
where not coalesce(q.excluded_from_marts, false)
