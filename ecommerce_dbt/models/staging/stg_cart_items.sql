with source as (
    select * from {{ source('raw', 'RAW_CART_ITEMS') }}
)

select
    cart_id,
    user_id,
    product_id,
    product_title,
    price,
    quantity,
    line_total,
    discount_pct,
    discounted_total,
    extracted_at
    
from source