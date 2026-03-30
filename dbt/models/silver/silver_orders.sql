with source as (
    select * from {{ ref('bronze_orders') }}
),

cleaned as (
    select
        -- Primary key
        ORDER_ID,

        -- Foreign keys
        CUSTOMER_ID,

        -- Order status
        lower(trim(ORDER_STATUS))                                    as order_status,

        -- Dates — cast from string to timestamp
        try_to_timestamp(ORDER_PURCHASE_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')     as order_purchase_timestamp,
        try_to_timestamp(ORDER_APPROVED_AT, 'YYYY-MM-DD HH24:MI:SS')            as order_approved_at,
        try_to_timestamp(ORDER_DELIVERED_CARRIER_DATE, 'YYYY-MM-DD HH24:MI:SS') as order_delivered_carrier_date,
        try_to_timestamp(ORDER_DELIVERED_CUSTOMER_DATE, 'YYYY-MM-DD HH24:MI:SS') as order_delivered_customer_date,
        try_to_timestamp(ORDER_ESTIMATED_DELIVERY_DATE, 'YYYY-MM-DD HH24:MI:SS') as order_estimated_delivery_date,

        -- Derived columns
        datediff(
            'day',
            try_to_timestamp(ORDER_PURCHASE_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp(ORDER_DELIVERED_CUSTOMER_DATE, 'YYYY-MM-DD HH24:MI:SS')
        )                                                            as actual_delivery_days,

        datediff(
            'day',
            try_to_timestamp(ORDER_PURCHASE_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp(ORDER_ESTIMATED_DELIVERY_DATE, 'YYYY-MM-DD HH24:MI:SS')
        )                                                            as estimated_delivery_days,

        -- Was the order delivered late?
        case
            when ORDER_DELIVERED_CUSTOMER_DATE > ORDER_ESTIMATED_DELIVERY_DATE then true
            else false
        end                                                          as is_late_delivery

    from source
    where ORDER_ID is not null
)

select * from cleaned