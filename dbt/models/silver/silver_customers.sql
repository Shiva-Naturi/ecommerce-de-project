with source as (
    select * from {{ ref('bronze_customers') }}
),

cleaned as (
    select
        CUSTOMER_ID,
        CUSTOMER_UNIQUE_ID,
        upper(trim(CUSTOMER_CITY))                as customer_city,
        upper(trim(CUSTOMER_STATE))               as customer_state,
        trim(CUSTOMER_ZIP_CODE_PREFIX)            as customer_zip_code
    from source
    where CUSTOMER_ID is not null
)

select * from cleaned