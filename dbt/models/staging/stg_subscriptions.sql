-- One row per subscription. A customer may hold up to three.
--
-- Standardizes format only -- see the note in stg_customers.sql.
--   D7: S00026's 'ACTIVE' is folded to 'active' here, so nothing downstream has to
--       remember to normalize before comparing.
--
-- Left deliberately alone, because repairing these is a business decision:
--   D8: S00034's end_date still precedes its start_date
--   D9: S00048's monthly_price is still negative
--   D6: S00011 still points at the non-existent C9999
-- All three are detected from the source in models/quarantine/invalid_subscriptions,
-- and the marts exclude or repair them according to the metric being computed.

with source as (

    select * from {{ source('billing_raw', 'subscriptions') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by subscription_id
            order by _dlt_id
        ) as _row_num
    from source

)

select
    trim(subscription_id)       as subscription_id,
    trim(customer_id)           as customer_id,
    lower(trim(plan_name))      as plan_name,
    lower(trim(status))         as status,
    monthly_price,
    start_date,
    end_date
from deduplicated
where _row_num = 1
