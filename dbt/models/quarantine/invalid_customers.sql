-- Customers breaching at least one data-quality rule, detected at the source.
--
-- Detection lives here, not in staging: staging is a typed passthrough that repairs
-- nothing, so this model reads raw and is the single place that decides what "wrong"
-- means for a customer.
--
-- One boolean per rule, never a single reason label -- a row can breach several at
-- once and each must stay visible.
--
-- excluded_from_marts is false throughout: no customer defect is severe enough to
-- withhold the row. A bad email does not invalidate real revenue. The marts repair
-- what they need (e.g. COALESCE country to 'UNKNOWN') and ignore the rest.

with source as (

    select * from {{ source('billing_raw', 'customers') }}

),

flagged as (

    -- One row per business key. The duplicate is reported by the flag, not by
    -- emitting the offending row twice -- the quarantine's grain must be stable.
    select distinct on (customer_id)
        customer_id,
        customer_name,
        email,
        country,
        created_at,

        count(*) over (partition by customer_id) > 1        as was_duplicated,       -- D1
        coalesce(trim(country), '') = ''                    as has_missing_country,  -- D2
        (email is null
         or email !~ '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$')
                                                            as has_invalid_email,    -- D3
        created_at > current_date                           as is_future_created     -- D4

    from source
    order by customer_id, _dlt_id

)

select
    *,
    false as excluded_from_marts
from flagged
where was_duplicated
   or has_missing_country
   or has_invalid_email
   or is_future_created
