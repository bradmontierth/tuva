{% macro apply_valid_date_filter(date_expression, alias, start_field='start_valid_date', end_field='end_valid_date') -%}
(
  {{ date_expression }} is null
  or (
    {{ date_expression }} >= {{ alias }}.{{ start_field }}
    and (
      {{ alias }}.{{ end_field }} is null
      or {{ date_expression }} <= {{ alias }}.{{ end_field }}
    )
  )
)
{%- endmacro %}
