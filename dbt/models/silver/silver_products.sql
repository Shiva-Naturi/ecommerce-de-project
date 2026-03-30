with source as (
    select * from {{ ref('bronze_products') }}
),

cleaned as (
    select
        PRODUCT_ID,
        lower(trim(PRODUCT_CATEGORY_NAME))        as product_category_name,
        try_to_number(PRODUCT_NAME_LENGHT)        as product_name_length,
        try_to_number(PRODUCT_DESCRIPTION_LENGHT) as product_description_length,
        try_to_number(PRODUCT_PHOTOS_QTY)         as product_photos_qty,
        try_to_number(PRODUCT_WEIGHT_G)           as product_weight_g,
        try_to_number(PRODUCT_LENGTH_CM)          as product_length_cm,
        try_to_number(PRODUCT_HEIGHT_CM)          as product_height_cm,
        try_to_number(PRODUCT_WIDTH_CM)           as product_width_cm
    from source
    where PRODUCT_ID is not null
)

select * from cleaned