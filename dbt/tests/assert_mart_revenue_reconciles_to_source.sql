-- MART CONTRACT -- fct_mrr and customer_ltv must report the same total revenue.
--
-- The two marts answer different questions from the same base (int_paid_invoices_eur):
-- one aggregates by month and plan, the other by customer. Their totals must agree. A
-- divergence means one of them lost or duplicated rows in its joins -- the classic
-- fan-out bug, which is easy to introduce and invisible in either mart alone.
--
-- Rounded to 2dp before comparing: both sides sum the same numeric(10,2) values, so any
-- genuine difference is far larger than a rounding artefact.
--
-- Returns rows only on failure.

with mrr as (
    select round(sum(mrr_eur), 2) as total from {{ ref('fct_mrr') }}
),

ltv as (
    select round(sum(total_revenue_eur), 2) as total from {{ ref('customer_ltv') }}
)

select
    mrr.total as fct_mrr_total_eur,
    ltv.total as customer_ltv_total_eur,
    mrr.total - ltv.total as difference_eur
from mrr
cross join ltv
where mrr.total is distinct from ltv.total
