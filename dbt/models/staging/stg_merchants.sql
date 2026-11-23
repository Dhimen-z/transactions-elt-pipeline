select
    merchant_id,
    mcc,
    category,
    city
from {{ source('raw', 'merchants') }}
