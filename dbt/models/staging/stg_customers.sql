-- One row per customer. Deduplicated; format standardized, values not repaired.

with source as (

    select * from {{ source('billing_raw', 'customers') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by _dlt_id
        ) as _row_num
    from source

)

select
    trim(customer_id)                       as customer_id,
    trim(customer_name)                     as customer_name,
    lower(trim(email))                      as email,
    nullif(upper(trim(country)), '')        as country,
    created_at
from deduplicated
where _row_num = 1
