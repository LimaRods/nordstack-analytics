{{ config(severity = 'warn') }}

-- Supporting test -- customer email should look like an address.
--
-- Runs against RAW at warn severity. email feeds none of the three required marts, so
-- a malformed one never justifies withholding a customer's revenue -- stg_customers
-- keeps the row and flags is_valid_email = false. This test exists so the defect is
-- counted in every build instead of disappearing quietly.
--
-- Expected: 1 row -- C0016, which holds the literal 'not-an-email' (D3).

select
    customer_id,
    customer_name,
    email
from {{ source('billing_raw', 'customers') }}
where email is null
   or email !~ '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$'
