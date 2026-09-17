{{ config(severity = 'warn') }}

-- Supporting test -- a subscription must have a positive recurring price.
--
-- monthly_price is the basis for "MRR lost" in the churn mart. A negative price would
-- make a cancellation ADD to revenue; a null one would drop the cancellation silently.
--
-- Runs against RAW at warn severity. The rows it finds are quarantined downstream as
-- non_positive_monthly_price.
--
-- Expected: 1 row -- S00048 at -99.00 on a starter plan priced at EUR 29 everywhere
-- else, so the value is wrong in both sign and magnitude (D9). It propagates into the
-- seven invoices that Test A reports.

select
    subscription_id,
    customer_id,
    plan_name,
    monthly_price
from {{ source('billing_raw', 'subscriptions') }}
where monthly_price is null
   or monthly_price <= 0
