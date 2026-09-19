-- Subscriptions with a data-quality issue, detected against the source.
-- excluded_from_marts is true only where the row cannot be attributed or priced.

with source as (

    -- was_duplicated is computed before any join. Computing it afterwards would count
    select
        *,
        count(*) over (partition by subscription_id) > 1 as was_duplicated
    from {{ source('billing_raw', 'subscriptions') }}

),

customer_keys as (

    select distinct customer_id
    from {{ source('billing_raw', 'customers') }}

),

flagged as (

    select distinct on (s.subscription_id)
        s.subscription_id,
        s.customer_id,
        s.plan_name,
        s.monthly_price,
        s.start_date,
        s.end_date,
        s.status,

        s.was_duplicated,
        c.customer_id is null                               as has_orphan_customer,
        s.status <> lower(trim(s.status))                   as has_unnormalized_status,
        (s.end_date is not null and s.end_date < s.start_date)
                                                            as has_invalid_end_date,
        (s.monthly_price is null or s.monthly_price <= 0)   as has_invalid_price

    from source as s
    left join customer_keys as c
        on s.customer_id = c.customer_id
    order by s.subscription_id

)

select
    *,
    (has_orphan_customer or has_invalid_price) as excluded_from_marts
from flagged
where was_duplicated
   or has_orphan_customer
   or has_unnormalized_status
   or has_invalid_end_date
   or has_invalid_price
