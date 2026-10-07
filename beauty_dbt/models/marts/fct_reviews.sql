-- Review fact table, grain = one review. Built incrementally: each run only processes
-- load_date partitions newer than what is already in the table.
{{ config(materialized='incremental', unique_key='review_id', incremental_strategy='delete+insert') }}

select
    r.review_id,
    r.product_id,
    p.brand_id,
    r.author_id,
    r.review_date,
    r.rating,
    r.is_recommended,
    r.helpfulness,
    r.total_feedback_count,
    r.total_pos_feedback_count,
    r.total_neg_feedback_count,
    r.price_usd_at_review,
    r.skin_type,
    r.skin_tone,
    r.review_title,
    r.review_text,
    r.load_date,
    r.batch_id
from {{ ref('stg_reviews') }} r
-- orphan reviews (unknown product) are logged by the raw relationships test, not loaded
join {{ ref('dim_products') }} p using (product_id)
{% if is_incremental() %}
where r.load_date > (select coalesce(max(load_date), date '1900-01-01') from {{ this }})
{% endif %}
