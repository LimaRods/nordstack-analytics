{{ config(severity = 'warn') }}

-- email feeds no mart, so this only makes the defect visible.

select
    customer_id,
    customer_name,
    email
from {{ source('billing_raw', 'customers') }}
where email is null
   or email !~ '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$'
