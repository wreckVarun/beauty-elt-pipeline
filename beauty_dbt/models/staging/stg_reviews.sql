{{ config(materialized='table') }}

-- Typed, de-duplicated reviews. Rows that cannot be used (no product, rating outside 1-5,
-- unparseable date) are dropped here; the raw-layer tests have already logged them.
with typed as (
    select
        md5(concat_ws('|', author_id, product_id, submission_time, coalesce(review_text, ''))) as review_id,
        trim(author_id)                                    as author_id,
        trim(product_id)                                   as product_id,
        try_cast(rating as integer)                        as rating,
        try_cast(try_cast(is_recommended as double) as integer) = 1 as is_recommended,
        try_cast(helpfulness as double)                    as helpfulness,
        try_cast(total_feedback_count as integer)          as total_feedback_count,
        try_cast(total_neg_feedback_count as integer)      as total_neg_feedback_count,
        try_cast(total_pos_feedback_count as integer)      as total_pos_feedback_count,
        try_cast(left(submission_time, 10) as date)        as review_date,
        nullif(trim(review_title), '')                     as review_title,
        nullif(trim(review_text), '')                      as review_text,
        nullif(skin_tone, '')                              as skin_tone,
        nullif(skin_type, '')                              as skin_type,
        nullif(eye_color, '')                              as eye_color,
        nullif(hair_color, '')                             as hair_color,
        try_cast(price_usd as double)                      as price_usd_at_review,
        cast(load_date as date)                            as load_date,
        _batch_id                                          as batch_id
    from {{ source('raw', 'reviews') }}
)

-- keep the first-landed copy of each duplicate review
select distinct on (review_id) *
from typed
where product_id is not null
  and rating between 1 and 5
  and review_date is not null
order by review_id, load_date
