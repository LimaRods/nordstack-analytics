-- Brief requirement 1: Monthly Recurring Revenue by month and by plan.
--
-- Grain: one row per (month, plan).
--
-- DEFINITION. MRR here is recognised revenue: the sum of PAID invoice amounts, in EUR,
-- attributed to the month the invoice was issued. The brief asks explicitly for MRR
-- "from paid invoices", so this is cash actually collected -- not contracted
-- subscription value, which would be sum(monthly_price) over active subscriptions and
-- would give a different (higher) number since failed and open invoices would count.
--
-- Because billing is monthly and each invoice equals its subscription's monthly_price,
-- the two definitions coincide for any subscription that pays on time. They diverge
-- exactly where payment failed -- which is the point of using the paid-invoice basis.
--
-- Months with no paid invoice for a plan simply do not appear; the series is not
-- gap-filled. S00040's missed December 2024 (E17) therefore shows as a dip rather than
-- a zero.

select
    invoice_month,
    plan_name,

    count(*)                          as paid_invoices,
    count(distinct subscription_id)   as paying_subscriptions,
    count(distinct customer_id)       as paying_customers,
    sum(amount_eur)                   as mrr_eur

from {{ ref('int_paid_invoices_eur') }}
group by invoice_month, plan_name
