with source as (
    select * from {{ source('bronze', 'ORDER_REVIEWS') }}
)
select * from source