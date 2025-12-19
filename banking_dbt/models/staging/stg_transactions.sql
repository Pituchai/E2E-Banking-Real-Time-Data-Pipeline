{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        v:id::string AS transaction_id,
        v:account_id::string AS account_id,
        v:amount::float AS amount,
        v:txn_type::string AS transaction_type,
        v:related_account_id::string AS related_account_id,
        v:created_at::timestamp_ntz AS created_at,
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        ROW_NUMBER() OVER (PARTITION BY v:id ORDER BY v:created_at DESC) AS rn
    FROM {{ source('raw', 'transactions') }}
)

SELECT
    transaction_id,
    account_id,
    amount,
    transaction_type,
    related_account_id,
    created_at,
    dbt_updated_at
FROM ranked
WHERE rn = 1 