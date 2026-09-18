-- Brief requirement 3: for each month, how many subscriptions were cancelled and the
-- MRR lost.
--
-- Grain: one row per cancellation month.
--
-- CHURN MONTH is the month of end_date -- the month the cancellation took effect.
--
-- MRR LOST is the sum of monthly_price over the cancelled subscriptions. The source
-- carries the recurring price directly, so no proxy (such as the last paid invoice) is
-- needed. Prices are already EUR; no FX conversion applies, unlike fct_mrr which
-- converts per invoice.
--
-- EXCLUSIONS, both inherited from int_subscriptions:
--   - subscriptions that cannot be attributed or priced (S00011, S00048) are gone
--     before this model sees them;
--   - S00034 is excluded from churn ONLY. Its end_date precedes its start_date and it
--     kept billing for 16 months afterwards, so the cancellation month is not knowable.
--     Its EUR 3,887 of paid revenue still appears in fct_mrr and customer_ltv. The cost
--     is one missing cancellation in 2025-03 -- documented, not silently absorbed.
--     See DISCOVERY.md section 5.1.
--
-- FUTURE MONTHS APPEAR HERE BY DESIGN. Three subscriptions carry cancellation dates in
-- 2027 (E16): scheduled cancellations that have not taken effect yet. They are real
-- commitments, so they are reported rather than filtered -- but a consumer charting
-- churn over time should not mistake them for realised churn.

select
    date_trunc('month', end_date)::date    as churn_month,

    count(*)                               as cancelled_subscriptions,
    count(distinct customer_id)            as churned_customers,
    sum(monthly_price)                     as mrr_lost_eur,
    sum(monthly_price * 2)                     as mrr_lost_eur_double,


    (date_trunc('month', end_date)::date > current_date) as is_future_cancellation

from {{ ref('int_subscriptions') }}
where is_churn_eligible
group by date_trunc('month', end_date)::date
