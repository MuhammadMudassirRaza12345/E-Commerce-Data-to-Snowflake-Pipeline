with source as (
    select * from {{ source('raw', 'RAW_CARTS') }}
)

select
    cart_id,
    user_id,
    total,
    discounted_total,
    total_products,
    total_quantity,
    extracted_at
from source 