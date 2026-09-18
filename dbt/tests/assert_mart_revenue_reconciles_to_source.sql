-- Mart contract: both marts aggregate the same base, so their totals must agree.
-- A divergence means a join fanned out. Returns rows only on failure.

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
