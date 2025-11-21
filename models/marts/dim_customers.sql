
with customers as (
    select
        *
    from {{ ref('stg_jaflle_shop_customers') }}
),

orders as (
    select
        *
    from {{ ref('stg_jaffle_shop_orders') }}
),
total_payments as (
    select
        customer_id,
        sum(p.amount) as lifetime_value
    from {{ ref('stg_stripe_payments') }} p 
    join {{ ref('stg_jaffle_shop_orders') }} o 
        on p.order_id = o.order_id
    group by customer_id
),

customer_orders as (
    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        co.first_order_date,
        co.most_recent_order_date,
        coalesce(co.number_of_orders, 0) as number_of_orders,
        tp.lifetime_value
    from customers c
    left join customer_orders co
        on c.customer_id = co.customer_id
    left join total_payments tp
        on c.customer_id = tp.customer_id

)

select * from final
