{% snapshot accounts_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='check',
      check_cols=['customer_id', 'account_type', 'currency'],
    )
}}

SELECT
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    loaded_at
FROM {{ ref('stg_accounts') }}

{% endsnapshot %}