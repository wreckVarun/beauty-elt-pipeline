-- One row per product from the most recent product snapshot, typed and cleaned.
with latest as (
    select *
    from {{ source('raw', 'products') }}
    where load_date = (select max(load_date) from {{ source('raw', 'products') }})
)

select distinct on (trim(product_id))
    trim(product_id)                                  as product_id,
    trim(product_name)                                as product_name,
    try_cast(brand_id as integer)                     as brand_id,
    trim(brand_name)                                  as brand_name,
    try_cast(price_usd as double)                     as price_usd,
    try_cast(sale_price_usd as double)                as sale_price_usd,
    try_cast(rating as double)                        as catalog_rating,
    try_cast(reviews as integer)                      as catalog_review_count,
    try_cast(loves_count as integer)                  as loves_count,
    nullif(trim(size), '')                            as size,
    primary_category,
    secondary_category,
    tertiary_category,
    try_cast(limited_edition as integer) = 1          as is_limited_edition,
    try_cast(new as integer) = 1                      as is_new,
    try_cast(online_only as integer) = 1              as is_online_only,
    try_cast(out_of_stock as integer) = 1             as is_out_of_stock,
    try_cast(sephora_exclusive as integer) = 1        as is_sephora_exclusive,
    cast(load_date as date)                           as snapshot_date
from latest
where product_id is not null
order by trim(product_id), _loaded_at desc
