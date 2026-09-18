{{ config(severity = 'warn') }}

-- monthly_price is the basis for MRR lost. A negative price would make a cancellation add
-- to revenue; a null one would drop it silently. Expected: S00048 at -99.00.

select
    subscription_id,
    customer_id,
    plan_name,
    monthly_price
from {{ source('billing_raw', 'subscriptions') }}
where monthly_price is null
   or monthly_price <= 0
