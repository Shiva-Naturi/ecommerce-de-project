with sellers as (
    select * from {{ ref('silver_sellers') }}
),

order_items as (
    select * from {{ ref('silver_order_items') }}
),

seller_metrics as (
    select
        SELLER_ID,
        count(distinct ORDER_ID)                      as total_orders,
        count(*)                                      as total_items_sold,
        round(avg(price), 2)                          as avg_item_price,
        round(sum(price), 2)                          as total_revenue
    from order_items
    group by SELLER_ID
)

select
    s.SELLER_ID                                       as seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_zip_code,
    sm.total_orders,
    sm.total_items_sold,
    sm.avg_item_price,
    sm.total_revenue,

    -- Seller tier based on revenue
    case
        when sm.total_revenue >= 100000 then 'platinum'
        when sm.total_revenue >= 50000  then 'gold'
        when sm.total_revenue >= 10000  then 'silver'
        else 'bronze'
    end                                               as seller_tier

from sellers s
left join seller_metrics sm using (SELLER_ID)