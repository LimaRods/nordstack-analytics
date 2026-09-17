-- Subscriptions breaching at least one data-quality rule, detected at the source.
--
-- excluded_from_marts separates two populations:
--   true  -> the row cannot be attributed or priced, so it must not reach any mart
--   false -> the row is usable; only a field is suspect, and the marts decide
--
-- S00034 is the second kind: its end_date precedes its start_date, yet it billed for
-- 16 more months (17 invoices, 13 paid, EUR 3,887). Its revenue is real, so it stays in
-- MRR and LTV -- the churn mart is the one that must ignore it. See DISCOVERY.md 5.1.

with source as (

    -- was_duplicated is computed HERE, before any join. Computing it after the join to
    -- customers would count join fan-out as source duplication: C0023 has two rows in
    -- raw.customers, which silently doubled its subscriptions S00029 and S00030 and
    -- reported them as duplicates they are not.
    select
        *,
        count(*) over (partition by subscription_id) > 1 as was_duplicated   -- D5
    from {{ source('billing_raw', 'subscriptions') }}

),

customer_keys as (

    -- DISTINCT is load-bearing: raw.customers contains C0023 twice, and joining it
    -- un-deduplicated would multiply every subscription belonging to that customer.
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
        c.customer_id is null                               as has_orphan_customer,     -- D6
        s.status <> lower(trim(s.status))                   as has_unnormalized_status, -- D7
        (s.end_date is not null and s.end_date < s.start_date)
                                                            as has_invalid_end_date,    -- D8
        (s.monthly_price is null or s.monthly_price <= 0)   as has_invalid_price        -- D9

    from source as s
    left join customer_keys as c
        on s.customer_id = c.customer_id
    order by s.subscription_id

)

select
    *,
    -- Only an unattributable or unpriceable subscription is withheld. Casing and a
    -- corrupt end_date are repairable downstream.
    (has_orphan_customer or has_invalid_price) as excluded_from_marts
from flagged
where was_duplicated
   or has_orphan_customer
   or has_unnormalized_status
   or has_invalid_end_date
   or has_invalid_price
