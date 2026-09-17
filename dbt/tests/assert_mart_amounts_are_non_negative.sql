-- MART CONTRACT -- no published figure may be negative.
--
-- Negative revenue or negative churned MRR would mean a quarantine rule failed: the
-- source's -99.00 price (S00048) and its seven invoices are the only way such a value
-- could arise, and all eight are excluded upstream.
--
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
