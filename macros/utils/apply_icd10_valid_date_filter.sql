{% macro apply_icd10_valid_date_filter(date_expression, alias, release_year_alias) -%}
(
  {{ apply_valid_date_filter(date_expression, alias) }}
  or (
    {{ date_expression }} is not null
    and {{ date_part('year', date_expression) }} > {{ release_year_alias }}.max_release_year
    and {{ alias }}.release_year = {{ release_year_alias }}.max_release_year
  )
)
{%- endmacro %}
