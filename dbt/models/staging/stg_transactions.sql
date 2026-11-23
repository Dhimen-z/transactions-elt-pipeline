select
    transaction_id,
    event_ts,
    cast(event_ts as date)          as event_date,
    extract(hour from event_ts)     as event_hour,
    customer_id,
    merchant_id,
    amount,
    upper(channel)                  as channel,
    upper(status)                   as status,
    status = 'APPROVED'             as is_approved,
    coalesce(is_fraud, false)       as is_fraud,
    _loaded_at
from {{ source('raw', 'transactions') }}
