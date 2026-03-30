with source as (
    select * from {{ ref('bronze_order_payments') }}
),

cleaned as (
    select
        ORDER_ID,
        try_to_number(PAYMENT_SEQUENTIAL)         as payment_sequential,
        lower(trim(PAYMENT_TYPE))                 as payment_type,
        try_to_number(PAYMENT_INSTALLMENTS)       as payment_installments,
        try_to_number(PAYMENT_VALUE, 10, 2)       as payment_value
    from source
    where ORDER_ID is not null
)

select * from cleaned