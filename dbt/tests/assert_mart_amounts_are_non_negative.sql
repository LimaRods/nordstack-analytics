-- Mart contract: no published figure may be negative. A negative value would mean a
-- quarantine rule failed -- S00048's -99.00 price and its invoices are the only source.
-- Returns rows only on failure.

select 'fct_mrr' as model, invoice_month::text as key, mrr_eur as value
from {{ ref('fct_mrr') }}
where mrr_eur < 0

union all

select 'customer_ltv', customer_id, total_revenue_eur
from {{ ref('customer_ltv') }}
where total_revenue_eur < 0

union all

select 'subscription_churn', churn_month::text, mrr_lost_eur
from {{ ref('subscription_churn') }}
where mrr_lost_eur <= 0
