{{ config(severity = 'warn') }}

-- The source stores an empty string, not NULL, so a generic not_null test would pass.

select
    customer_id,
    customer_name,
    country
from {{ source('billing_raw', 'customers') }}
where coalesce(trim(country), '') = ''
