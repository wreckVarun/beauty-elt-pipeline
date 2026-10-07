-- Brand dimension: one row per brand with catalog-level attributes.
select
    brand_id,
    any_value(brand_name)                    as brand_name,
    count(*)                                 as n_products,
    round(median(price_usd), 2)              as median_price_usd,
    round(avg(catalog_rating), 3)            as avg_catalog_rating,
    sum(loves_count)                         as total_loves
from {{ ref('stg_products') }}
where brand_id is not null
group by brand_id
