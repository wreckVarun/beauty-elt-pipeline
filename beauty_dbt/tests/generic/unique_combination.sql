{% test unique_combination(model, combination) %}
-- Every group of rows that shares the same values for all columns in `combination`.
select {{ combination | join(', ') }}, count(*) as n_rows
from {{ model }}
group by {{ combination | join(', ') }}
having count(*) > 1
{% endtest %}
