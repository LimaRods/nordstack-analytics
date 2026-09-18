-- Monthly Recurring Revenue by month and plan. Grain: one row per (month, plan).
--
-- MRR here is recognised revenue: paid invoice amounts in EUR, attributed to the month
-- the invoice was issued. The brief asks for MRR from paid invoices, so this is cash
-- collected, not contracted value. The two coincide for a subscription paying on time
-- and diverge exactly where payment failed -- which is the point of the paid basis.
--
-- Months with no paid invoice for a plan are absent rather than zero-filled.

select
    invoice_month,
    plan_name,

    count(*)                          as paid_invoices,
    count(distinct subscription_id)   as paying_subscriptions,
    count(distinct customer_id)       as paying_customers,
    sum(amount_eur)                   as mrr_eur

from {{ ref('int_paid_invoices_eur') }}
group by invoice_month, plan_name
