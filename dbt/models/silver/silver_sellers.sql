with source as (
    select * from {{ ref('bronze_sellers') }}
),

cleaned as (
    select
        SELLER_ID,
        trim(SELLER_ZIP_CODE_PREFIX)              as seller_zip_code,
        upper(trim(SELLER_CITY))                  as seller_city,
        upper(trim(SELLER_STATE))                 as seller_state
    from source
    where SELLER_ID is not null
)

select * from cleaned