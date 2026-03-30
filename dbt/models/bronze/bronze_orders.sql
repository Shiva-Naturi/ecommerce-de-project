with source as (
    select * from {{ source('bronze', 'ORDERS') }}
)

select * from source