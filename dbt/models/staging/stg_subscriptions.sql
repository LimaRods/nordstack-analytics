-- One row per subscription. Deduplicated; values left as they arrive.

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
