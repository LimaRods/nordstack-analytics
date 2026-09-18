{{ config(severity = 'warn') }}

-- monthly_price is the basis for MRR lost, so a negative one would add to revenue.

select
    subscription_id,
    customer_id,
    plan_name,
    monthly_price
from {{ source('billing_raw', 'subscriptions') }}
where monthly_price is null
   or monthly_price <= 0
