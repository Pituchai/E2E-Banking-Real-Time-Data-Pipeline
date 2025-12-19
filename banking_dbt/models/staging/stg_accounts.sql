{{ config(materialized='view') }}
--- with clause expression --- 
with ranked as (
    select 
        v:id::string as account_id,
        v:customer_id::string as customer_id,
        v:account_type::string as account_type,
        v:balance::float as balance,
        v:currency::string as currency,
        v:created_at::timestamp_ntz as created_at,
        current_timestamp() as loaded_at,
        row_number() over (
            partition by v:id::string 
            order by v:created_at::timestamp_ntz desc
        ) as row_num
    from {{ source('raw', 'accounts') }} as v    
)

select 
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    loaded_at
from ranked
--- provide the latest record only ---
where row_num = 1