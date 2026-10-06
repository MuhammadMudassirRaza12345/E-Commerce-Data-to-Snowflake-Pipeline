with source as (
    select * from {{ source('raw', 'RAW_PRODUCTS') }}
)

select
    product_id,
    title           as product_name,
    description,
    price,
    discount_percentage  as discount_pct,
    rating,
    stock,
    brand,
    category,
    thumbnail_url,
    extracted_at
from source 