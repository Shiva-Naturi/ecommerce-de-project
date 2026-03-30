with products as (
    select * from {{ ref('silver_products') }}
),

translation as (
    select * from {{ ref('silver_product_category_translation') }}
)

select
    p.PRODUCT_ID                                      as product_id,
    coalesce(t.product_category_name_english,
             p.product_category_name,
             'unknown')                               as product_category,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    -- Derived volume
    p.product_length_cm *
    p.product_height_cm *
    p.product_width_cm                                as product_volume_cm3

from products p
left join translation t
    on p.product_category_name = t.product_category_name