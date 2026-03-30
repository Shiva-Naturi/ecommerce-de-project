with source as (
    select * from {{ ref('bronze_order_items') }}
),

cleaned as (
    select
        ORDER_ID,
        try_to_number(ORDER_ITEM_ID)              as order_item_id,
        PRODUCT_ID,
        SELLER_ID,
        try_to_timestamp(SHIPPING_LIMIT_DATE, 'YYYY-MM-DD HH24:MI:SS') as shipping_limit_date,
        try_to_number(PRICE, 10, 2)               as price,
        try_to_number(FREIGHT_VALUE, 10, 2)       as freight_value,
        try_to_number(PRICE, 10, 2) +
        try_to_number(FREIGHT_VALUE, 10, 2)       as total_item_value
    from source
    where ORDER_ID is not null
)

select * from cleaned