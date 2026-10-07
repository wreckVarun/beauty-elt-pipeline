-- Complaint mix per brand from the LLM-tagged review sample.
with tagged as (
    select f.brand_id, t.sentiment, t.complaint_type, t.labeler
    from {{ source('llm', 'review_tags') }} t
    join {{ ref('fct_reviews') }} f using (review_id)
),

per_type as (
    select
        brand_id,
        labeler,
        complaint_type,
        count(*)                                         as n_reviews,
        count(*) filter (where sentiment = 'negative')   as n_negative
    from tagged
    group by brand_id, labeler, complaint_type
)

select
    p.brand_id,
    b.brand_name,
    p.labeler,
    p.complaint_type,
    p.n_reviews,
    p.n_negative,
    sum(p.n_reviews) over (partition by p.brand_id, p.labeler)                      as n_tagged_for_brand,
    round(p.n_reviews / sum(p.n_reviews) over (partition by p.brand_id, p.labeler), 4) as share_of_brand_reviews
from per_type p
join {{ ref('dim_brands') }} b using (brand_id)
