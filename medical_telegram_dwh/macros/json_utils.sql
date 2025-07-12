{% macro extract_json_field(column, field) %}
  {{ column }}->>'{{ field }}'
{% endmacro %}