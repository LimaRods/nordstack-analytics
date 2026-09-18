-- Cancellations and MRR lost per month. Grain: one row per cancellation month.
--
-- Churn month is the month of end_date. MRR lost is the sum of monthly_price -- the
-- source carries the recurring price directly, so no proxy is needed, and prices are
-- already EUR.
--
-- Exclusions are inherited from int_subscriptions: rows that cannot be attributed or
-- priced never arrive, and S00034 is excluded from churn only, because its cancellation
-- month is not knowable. 49 of 52 cancellations are counted.
--
-- Future months appear by design: three subscriptions carry 2027 cancellation dates.
-- They are real commitments, so they are reported and flagged rather than filtered.

select
    date_trunc('month', end_date)::date    as churn_month,

    count(*)                               as cancelled_subscriptions,
    count(distinct customer_id)            as churned_customers,
    sum(monthly_price)                     as mrr_lost_eur,

    (date_trunc('month', end_date)::date > current_date) as is_future_cancellation

from {{ ref('int_subscriptions') }}
where is_churn_eligible
group by date_trunc('month', end_date)::date
