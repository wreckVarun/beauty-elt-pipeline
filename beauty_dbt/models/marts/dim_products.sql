-- Product dimension (conformed to dim_brands via brand_id).
select
    product_id,
    product_name,
    brand_id,
    primary_category,
    secondary_category,
    tertiary_category,
    price_usd,
    case
        when price_usd < 25  then '1. <$25'
        when price_usd < 50  then '2. $25-50'
        when price_usd < 100 then '3. $50-100'
        when price_usd >= 100 then '4. $100+'
    end                                      as price_band,
    size,
    catalog_rating,
    catalog_review_count,
    loves_count,
    is_limited_edition,
    is_new,
    is_online_only,
    is_sephora_exclusive,
    snapshot_date
from {{ ref('stg_products') }}
