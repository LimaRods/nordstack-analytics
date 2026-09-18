-- MRR by month and plan. Recognised revenue: paid invoices only, so open and failed
-- invoices count for nothing.

select
    invoice_month,
    plan_name,

    count(*)                          as paid_invoices,
    count(distinct subscription_id)   as paying_subscriptions,
    count(distinct customer_id)       as paying_customers,
    sum(amount_eur)                   as mrr_eur

from {{ ref('int_paid_invoices_eur') }}
group by invoice_month, plan_name
