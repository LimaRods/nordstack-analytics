{{ config(severity = 'warn') }}

-- A customer cannot be created in the future, nor after their own subscription began.

select
    c.customer_id,
    c.customer_name,
    c.created_at,
    min(s.start_date) as earliest_subscription_start
from {{ source('billing_raw', 'customers') }} as c
left join {{ source('billing_raw', 'subscriptions') }} as s
    on c.customer_id = s.customer_id
group by c.customer_id, c.customer_name, c.created_at
having c.created_at > current_date
    or c.created_at > min(s.start_date)
