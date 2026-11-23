select merchant_id, mcc, category, city
from {{ ref('stg_merchants') }}
