with source as (
    select * from {{ ref('bronze_product_category_translation') }}
),

cleaned as (
    select
        lower(trim(PRODUCT_CATEGORY_NAME))         as product_category_name,
        lower(trim(PRODUCT_CATEGORY_NAME_ENGLISH)) as product_category_name_english
    from source
    where PRODUCT_CATEGORY_NAME is not null
)

select * from cleaned