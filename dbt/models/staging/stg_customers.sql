-- One row per customer.
--
-- Staging STANDARDIZES FORMAT; it does not repair values.
--   standardize -> casing, whitespace, types: safe, reversible, loses no information
--   repair      -> substituting a value the source never had: a business decision,
--                  so it belongs in the mart that needs it
--
-- This is safe only because models/quarantine/ detects defects from the SOURCE, not
-- from here. The audit trail is independent of whatever staging does, so normalizing
-- can no longer hide anything.
--
-- Left deliberately alone: a blank country stays blank (the marts COALESCE it to
-- 'UNKNOWN' where they report on country) and a malformed email stays malformed.
--
-- Deduplication is kept: a duplicate row is a grain problem, not a formatting one.
-- D1 (C0023) arrives twice, byte-identical, so collapsing it is lossless.

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
