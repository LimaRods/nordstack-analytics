{{ config(severity = 'warn') }}

-- country is a required dimension of the LTV mart. The source stores an empty string,
-- not NULL, so a generic not_null test would pass and miss it. Expected: C0008.

select
    customer_id,
    customer_name,
    country
from {{ source('billing_raw', 'customers') }}
where coalesce(trim(country), '') = ''
