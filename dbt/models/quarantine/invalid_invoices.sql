-- Invoices breaching at least one data-quality rule, detected against the source.
--
-- Status is normalized inside the rule expressions because staging has not run yet:
-- an exact match on 'paid' would miss I000451's 'PAID ' and wrongly clear it.
--
-- excluded_from_marts separates two populations:
--   true  -> unattributable, or the amount is unusable: must not reach revenue
--   false -> usable; only the formatting is off, and the marts repair it

with source as (

    select * from {{ source('billing_raw', 'invoices') }}

),

subscriptions as (

    -- DISTINCT guards against S00006, which appears twice at source: joining it
    -- un-deduplicated would double every invoice on that subscription.
    select distinct subscription_id from {{ source('billing_raw', 'subscriptions') }}

),

quarantined_subscriptions as (

    select subscription_id
    from {{ ref('invalid_subscriptions') }}
    where excluded_from_marts

),

flagged as (

    select
        i.invoice_id,
        i.subscription_id,
        i.invoice_date,
        i.amount,
        i.currency,
        i.status,

        s.subscription_id is null                       as has_orphan_subscription,
        (lower(trim(i.status)) = 'paid' and i.amount is null)
                                                        as has_null_paid_amount,
        coalesce(i.amount <= 0, false)                   as has_non_positive_amount,
        i.status <> lower(trim(i.status))                as has_unnormalized_status,
        upper(trim(i.currency)) not in ('EUR', 'SEK')    as has_unsupported_currency,
        q.subscription_id is not null                    as has_excluded_subscription

    from source as i
    left join subscriptions as s
        on i.subscription_id = s.subscription_id
    left join quarantined_subscriptions as q
        on i.subscription_id = q.subscription_id

)

select
    *,
    (has_orphan_subscription
     or has_null_paid_amount
     or has_non_positive_amount
     or has_unsupported_currency
     or has_excluded_subscription) as excluded_from_marts
from flagged
where has_orphan_subscription
   or has_null_paid_amount
   or has_non_positive_amount
   or has_unnormalized_status
   or has_unsupported_currency
   or has_excluded_subscription
