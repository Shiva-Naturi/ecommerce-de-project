with source as (
    select * from {{ source('bronze', 'ORDER_PAYMENTS') }}
)
select * from source