{#
  Adds a column to a select-list only if it does not already
  exist on the provided relation. Emits a leading comma when adding.

  Usage in a SELECT list:
    select
      src.*
      {{ ensure_optional_column(ref('eligibility'), 'medicare_part_b_enrollment_start_date', 'date') }}
    from {{ ref('eligibility') }} as src

  This keeps downstream models stable by guaranteeing the column
  exists (as NULL typed) without forcing every connector to implement it immediately.
#}

{% macro ensure_optional_column(relation, column_name, data_type) %}
  {# Build a lowercase set of existing column names for the relation. #}
  {% set existing_cols = [] %}
  {% if execute %}
    {% set rel = relation %}
    {# Only attempt to introspect if the relation actually exists #}
    {% set rel_exists = adapter.get_relation(database=rel.database, schema=rel.schema, identifier=rel.identifier) is not none %}
    {% if rel_exists %}
      {% set cols = adapter.get_columns_in_relation(rel) %}
      {% for c in cols %}
        {% if c.name is defined %}
          {% do existing_cols.append(c.name | lower) %}
        {% elif c.column is defined %}
          {% do existing_cols.append(c.column | lower) %}
        {% endif %}
      {% endfor %}
    {% endif %}
  {% endif %}

  {% if execute and (column_name | lower) in existing_cols %}
    {# Column exists on relation; no-op #}
  {% else %}
    , cast(null as {{ data_type }}) as {{ column_name }}
  {% endif %}
{% endmacro %}

