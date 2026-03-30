with source as (
    select * from {{ source('bronze', 'PRODUCTS') }}
)
select * from source