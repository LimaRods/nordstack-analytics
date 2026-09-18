-- Subscriptions the marts may consume. Rows that cannot be attributed or priced are
-- dropped; a row with an untrustworthy end_date is kept and flagged, so only the
-- churn mart excludes it.

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

    (s.status = 'cancelled'
     and s.end_date is not null
     and not coalesce(q.has_invalid_end_date, false)) as is_churn_eligible

from subscriptions as s
left join quarantined as q
    on s.subscription_id = q.subscription_id
where not coalesce(q.excluded_from_marts, false)
