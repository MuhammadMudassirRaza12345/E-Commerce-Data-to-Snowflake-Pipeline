with stg as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    product_name,
    description,
    brand,
    category,
    price,
    discount_pct,
    round(price * (1 - discount_pct / 100), 2)  as discounted_price,
    rating,
    stock,
    thumbnail_url
from stg 