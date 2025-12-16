{%- macro data_quality_relation_has_column(relation, column_name) -%}
    {%- if relation is none -%}
        {{ return(false) }}
    {%- endif -%}

    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {%- for column in columns -%}
        {%- if column.name | lower == column_name | lower -%}
            {{ return(true) }}
        {%- endif -%}
    {%- endfor -%}

    {{ return(false) }}
{%- endmacro -%}

