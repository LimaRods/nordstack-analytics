-- One row per customer. Standardizes format; does not repair values -- a blank country
-- stays blank, a malformed email stays malformed. Substituting a value the source never
-- had is a business decision and belongs in the mart that needs it.
--
-- Deduplication is the exception: a duplicate row is a grain problem, and C0023's two
-- rows are byte-identical, so collapsing them loses nothing.

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
