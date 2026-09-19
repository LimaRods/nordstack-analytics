-- Customers with a data-quality issue, detected against the source.
-- No customer defect is severe enough to withhold the row from the marts.

with source as (

    select * from {{ source('billing_raw', 'customers') }}

),

flagged as (

    select distinct on (customer_id)
        customer_id,
        customer_name,
        email,
        country,
        created_at,

        count(*) over (partition by customer_id) > 1        as was_duplicated,
        coalesce(trim(country), '') = ''                    as has_missing_country,
        (email is null
         or email !~ '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$')
                                                            as has_invalid_email,
        created_at > current_date                           as is_future_created

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
