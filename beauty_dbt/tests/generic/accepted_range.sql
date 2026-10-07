{% test accepted_range(model, column_name, min_value=none, max_value=none, cast_as='double') %}
-- Rows whose value cannot be cast, or falls outside [min_value, max_value]. Nulls are ignored
-- (pair with not_null if they matter).
select *
from {{ model }}
where {{ column_name }} is not null
  and (
        try_cast({{ column_name }} as {{ cast_as }}) is null
        {% if min_value is not none %} or try_cast({{ column_name }} as {{ cast_as }}) < {{ min_value }} {% endif %}
        {% if max_value is not none %} or try_cast({{ column_name }} as {{ cast_as }}) > {{ max_value }} {% endif %}
      )
{% endtest %}
