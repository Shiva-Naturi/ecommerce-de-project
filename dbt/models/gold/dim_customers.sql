with customers as (
    select * from {{ ref('silver_customers') }}
),

orders as (
    select * from {{ ref('silver_orders') }}
),

-- Deduplicate customers keeping one row per unique customer
deduplicated as (
    select distinct
        CUSTOMER_UNIQUE_ID,
        first_value(customer_city) over (
            partition by CUSTOMER_UNIQUE_ID
            order by CUSTOMER_UNIQUE_ID
        )                                             as customer_city,
        first_value(customer_state) over (
            partition by CUSTOMER_UNIQUE_ID
            order by CUSTOMER_UNIQUE_ID
        )                                             as customer_state,
        first_value(customer_zip_code) over (
            partition by CUSTOMER_UNIQUE_ID
            order by CUSTOMER_UNIQUE_ID
        )                                             as customer_zip_code
    from customers
),

customer_metrics as (
    select
        c.CUSTOMER_UNIQUE_ID,
        count(distinct o.ORDER_ID)                    as total_orders,
        min(o.order_purchase_timestamp)               as first_order_date,
        max(o.order_purchase_timestamp)               as last_order_date,
        datediff(
            'day',
            min(o.order_purchase_timestamp),
            max(o.order_purchase_timestamp)
        )                                             as customer_lifetime_days
    from customers c
    left join orders o on c.CUSTOMER_ID = o.CUSTOMER_ID
    group by c.CUSTOMER_UNIQUE_ID
)

select
    d.CUSTOMER_UNIQUE_ID                              as customer_id,
    d.customer_city,
    d.customer_state,
    d.customer_zip_code,
    coalesce(cm.total_orders, 0)                      as total_orders,
    cm.first_order_date,
    cm.last_order_date,
    coalesce(cm.customer_lifetime_days, 0)            as customer_lifetime_days,
    case
        when coalesce(cm.total_orders, 0) = 1  then 'one_time'
        when coalesce(cm.total_orders, 0) <= 3 then 'occasional'
        when coalesce(cm.total_orders, 0) > 3  then 'loyal'
    end                                               as customer_segment

from deduplicated d
left join customer_metrics cm using (CUSTOMER_UNIQUE_ID)