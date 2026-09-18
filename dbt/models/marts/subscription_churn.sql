-- Cancellations and MRR lost per month. Scheduled future cancellations are reported
-- and flagged rather than filtered.

select
    date_trunc('month', end_date)::date    as churn_month,

    count(*)                               as cancelled_subscriptions,
    count(distinct customer_id)            as churned_customers,
    sum(monthly_price)                     as mrr_lost_eur,

    (date_trunc('month', end_date)::date > current_date) as is_future_cancellation

from {{ ref('int_subscriptions') }}
where is_churn_eligible
group by date_trunc('month', end_date)::date
