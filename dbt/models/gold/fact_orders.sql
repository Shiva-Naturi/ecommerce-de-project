with orders as (
    select * from {{ ref('silver_orders') }}
),

order_items as (
    select
        ORDER_ID,
        count(*)                                      as total_items,
        round(sum(price), 2)                          as total_price,
        round(sum(freight_value), 2)                  as total_freight,
        round(sum(total_item_value), 2)               as total_order_value
    from {{ ref('silver_order_items') }}
    group by ORDER_ID
),

payments as (
    select
        ORDER_ID,
        round(sum(payment_value), 2)                  as total_payment_value,
        count(distinct payment_type)                  as payment_methods_used,
        max(payment_installments)                     as max_installments,
        listagg(distinct payment_type, ', ')
            within group (order by payment_type)      as payment_types
    from {{ ref('silver_order_payments') }}
    group by ORDER_ID
),

reviews as (
    select
        ORDER_ID,
        round(avg(review_score), 2)                   as avg_review_score
    from {{ ref('silver_order_reviews') }}
    group by ORDER_ID
),

customers as (
    select
        CUSTOMER_ID,
        CUSTOMER_UNIQUE_ID
    from {{ ref('silver_customers') }}
)

select
    -- Keys
    o.ORDER_ID                                        as order_id,
    c.CUSTOMER_UNIQUE_ID                              as customer_id,
    o.order_purchase_timestamp::date                  as order_date,

    -- Order details
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Delivery metrics
    o.actual_delivery_days,
    o.estimated_delivery_days,
    o.is_late_delivery,

    -- Financial metrics
    oi.total_items,
    oi.total_price,
    oi.total_freight,
    coalesce(oi.total_order_value, 0)               as total_order_value,
    p.total_payment_value,
    p.payment_types,
    p.payment_methods_used,
    p.max_installments,

    -- Review
    r.avg_review_score

from orders o
left join order_items oi  on o.ORDER_ID = oi.ORDER_ID
left join payments p      on o.ORDER_ID = p.ORDER_ID
left join reviews r       on o.ORDER_ID = r.ORDER_ID
left join customers c     on o.CUSTOMER_ID = c.CUSTOMER_ID