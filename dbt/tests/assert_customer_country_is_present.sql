{{ config(severity = 'warn') }}

-- Supporting test -- every customer must carry a country.
--
-- Runs against RAW at warn severity. country is a required dimension of the LTV mart,
-- so a blank one would silently group with a real country or vanish from a breakdown.
-- stg_customers normalizes it to 'UNKNOWN' and raises has_missing_country, so the row
-- is kept and the gap stays visible instead of being papered over.
--
-- Note the source stores an EMPTY STRING, not NULL, which is why a generic not_null
-- test would pass here and miss the defect entirely.
--
-- Expected: 1 row -- C0008 (D2).

select
    customer_id,
    customer_name,
    country
from {{ source('billing_raw', 'customers') }}
where coalesce(trim(country), '') = ''
