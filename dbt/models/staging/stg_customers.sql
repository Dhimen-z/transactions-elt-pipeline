select
    customer_id,
    lower(segment)              as segment,
    city,
    cast(created_at as date)    as customer_since
from {{ source('raw', 'customers') }}
