with source as (
    select * from {{ source('bronze', 'SELLERS') }}
)
select * from source