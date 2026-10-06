with source as (
    select * from {{ source('raw', 'RAW_USERS') }}
)

select
    user_id,
    first_name,
    last_name,
    first_name || ' ' || last_name  as full_name,
    email,
    phone,
    gender,
    age,
    city,
    country,
    company,
    extracted_at
from source 