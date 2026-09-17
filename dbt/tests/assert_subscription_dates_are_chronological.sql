{{ config(severity = 'warn') }}

-- SINGULAR TEST B -- subscription dates must be chronologically valid.
--
-- A subscription cannot end before it starts. Such a row would place a cancellation in
-- a month preceding the subscription's own existence, producing a churn figure no
-- billing history supports.
--
-- Runs against RAW at warn severity, so the defect is reported where it originates
-- rather than after staging has already repaired it. stg_subscriptions nulls the
-- impossible end_date, which is exactly why this test must read the source: against
-- staging it could never see anything.
--
-- Expected: 1 row -- S00034 (start 2025-03-29, end 2025-03-19, yet it kept billing for
-- 16 more months: 17 invoices, 13 paid, EUR 3,887). See DISCOVERY.md section 5.1.

select
    subscription_id,
    customer_id,
    start_date,
    end_date,
    status
from {{ source('billing_raw', 'subscriptions') }}
where end_date is not null
  and end_date < start_date
