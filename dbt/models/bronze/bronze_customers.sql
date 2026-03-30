with source as (
    select * from {{ source('bronze', 'CUSTOMERS') }}
)
select * from source