{{ config(severity = 'warn') }}

-- A subscription cannot end before it starts.

select
    subscription_id,
    customer_id,
    start_date,
    end_date,
    status
from {{ source('billing_raw', 'subscriptions') }}
where end_date is not null
  and end_date < start_date
