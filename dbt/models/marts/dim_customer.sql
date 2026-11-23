select customer_id, segment, city, customer_since
from {{ ref('stg_customers') }}
