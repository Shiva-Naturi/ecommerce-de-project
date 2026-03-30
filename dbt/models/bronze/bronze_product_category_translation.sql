with source as (
    select * from {{ source('bronze', 'PRODUCT_CATEGORY_TRANSLATION') }}
)
select * from source