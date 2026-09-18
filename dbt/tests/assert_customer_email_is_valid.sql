{{ config(severity = 'warn') }}

-- email feeds no mart, so a malformed one never justifies withholding revenue. This
-- exists so the defect is counted in every build. Expected: C0016, 'not-an-email'.

select
    customer_id,
    customer_name,
    email
from {{ source('billing_raw', 'customers') }}
where email is null
   or email !~ '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$'
