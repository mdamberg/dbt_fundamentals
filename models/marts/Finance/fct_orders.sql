select
    so.order_id,
    sc.customer_id,
    p.amount
from {{ ref('stg_jaffle_shop_orders') }} so
join {{ ref('stg_jaffle_shop_customers') }} sc 
    on so.customer_id = sc.customer_id
join {{ ref('stg_stripe_payments') }} p
    on so.order_id = p.order_id
    