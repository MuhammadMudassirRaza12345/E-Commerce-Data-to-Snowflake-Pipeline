with stg as (
    select * from {{ ref('stg_users') }}
)

select
    user_id,
    full_name,
    first_name,
    last_name,
    email,
    phone,
    gender,
    age,
    city,
    country,
    company
from stg