-- One row per customer. Deduplicated and standardized, with one light repair: a blank
-- country becomes 'UNKNOWN'. Safe because data_issues reads raw, so C0008
-- is still flagged there. Amounts and dates are never substituted.

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
    coalesce(nullif(upper(trim(country)), ''), 'UNKNOWN')   as country,
    created_at
from deduplicated
where _row_num = 1
