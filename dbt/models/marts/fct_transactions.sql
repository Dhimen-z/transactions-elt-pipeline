{{ config(materialized='incremental', unique_key='transaction_id', incremental_strategy='delete+insert') }}

select
    t.transaction_id,
    t.event_ts,
    t.event_date,
    t.event_hour,
    t.customer_id,
    t.merchant_id,
    t.channel,
    t.amount,
    t.is_approved,
    t.is_fraud,
    t._loaded_at
from {{ ref('stg_transactions') }} t
{% if is_incremental() %}
where t._loaded_at > (select coalesce(max(_loaded_at), timestamp '1900-01-01') from {{ this }})
{% endif %}
