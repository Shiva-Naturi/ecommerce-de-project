with source as (
    select * from {{ source('bronze', 'ORDER_ITEMS') }}
)
select * from source