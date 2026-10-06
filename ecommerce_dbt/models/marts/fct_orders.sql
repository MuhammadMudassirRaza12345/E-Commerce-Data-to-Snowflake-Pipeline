with items as (
    select * from {{ ref('stg_cart_items') }}
),

carts as (
    select * from {{ ref('stg_carts') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

products as (
    select * from {{ ref('dim_products') }}
)

select
    i.cart_id,
    i.user_id,
    c.full_name          as customer_name,
    c.email,
    c.city,
    c.country,
    i.product_id,
    p.product_name,
    p.brand,
    p.category,
    i.quantity,
    i.price,
    i.line_total,
    i.discount_pct,
    i.discounted_total,
    ca.total             as cart_total,
    ca.discounted_total  as cart_discounted_total,
    ca.total_products,
    ca.total_quantity,
    i.extracted_at       as order_date
from items i
left join carts     ca  on i.cart_id   = ca.cart_id
left join customers c   on i.user_id   = c.user_id
left join products  p   on i.product_id = p.product_id 