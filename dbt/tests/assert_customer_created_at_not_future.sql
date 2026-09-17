{{ config(severity = 'warn') }}

-- Supporting test -- a customer should not be created in the future, nor after its own
-- subscription started.
--
-- Runs against RAW at warn severity. The signup date affects no required metric, and
-- clamping or dropping the row would destroy evidence of a real source-system problem.
--
-- Expected: 1 row -- C0041, dated 2027-03-15: in the future, and nearly three years
-- AFTER its own subscription S00054 started on 2024-06-24 (D4). A customer cannot be
-- created after they subscribe, so the row is reported by either clause.
--
-- Note the first clause is time-dependent (current_date), so it stops matching once the
-- clock passes 2027-03-15. The subscription comparison is stable and keeps the defect
-- detectable regardless; a production version would compare against a fixed as-of date
-- captured at load time.

select
    c.customer_id,
    c.customer_name,
    c.created_at,
    min(s.start_date) as earliest_subscription_start
from {{ source('billing_raw', 'customers') }} as c
left join {{ source('billing_raw', 'subscriptions') }} as s
    on c.customer_id = s.customer_id
group by c.customer_id, c.customer_name, c.created_at
having c.created_at > current_date
    or c.created_at > min(s.start_date)
