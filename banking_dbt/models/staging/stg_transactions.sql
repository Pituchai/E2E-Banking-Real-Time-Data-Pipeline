{{ config(materialized='view') }}

SELECT
    v:id::string AS transaction_id,
    v:account_id::string AS account_id,
    v:amount::float AS amount,
    v:txn_type::string AS transaction_type,
    v:related_account_id::string AS related_account_id,
    v:created_at::timestamp_ntz AS created_at,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ source('raw', 'transactions') }} 