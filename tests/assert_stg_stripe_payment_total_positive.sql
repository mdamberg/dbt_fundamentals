select
    order_id,
    sum(amount) as total_amount
from {{ ref('stg_stripe_payment') }}
group by order_id
having total_amount < 0;