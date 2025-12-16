{{ config(materialized='table') }}

{% set package_name = 'the_tuva_project' %}
{% set final_nodes = [] %}
{% set seen_aliases = [] %}
{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model'
        and node.package_name == package_name
        and '/final/' in node.original_file_path %}
        {% set path_parts = node.original_file_path.split('/') %}
        {% if path_parts | length > 1 %}
            {% set mart_name = path_parts[1] %}
        {% else %}
            {% set mart_name = none %}
        {% endif %}
        {% if mart_name is not none and node.alias not in seen_aliases %}
            {% do seen_aliases.append(node.alias) %}
            {% do final_nodes.append({'name': node.name, 'alias': node.alias, 'mart': mart_name}) %}
        {% endif %}
    {% endif %}
{% endfor %}
{% set final_nodes = final_nodes | sort(attribute='alias') %}

{% set base_prefix = 'base_' %}
{% set head_prefix = 'head_' %}

{% if target.database is none %}
    {% set database_name = this.database %}
{% else %}
    {% set database_name = target.database %}
{% endif %}

{% set metric_definitions = [] %}
{% for node in final_nodes %}
    {% do metric_definitions.append({
        'metric_name': 'row_count',
        'mart': node.mart,
        'table_name': node.alias,
        'schema_suffix': node.mart,
        'expression': 'COUNT(*)'
    }) %}
{% endfor %}
{% do metric_definitions.extend([
    {
        'metric_name': 'sum_paid_amount',
        'mart': 'core',
        'table_name': 'medical_claim',
        'schema_suffix': 'core',
        'expression': 'COALESCE(SUM(paid_amount), 0)'
    },
    {
        'metric_name': 'sum_paid_amount',
        'mart': 'core',
        'table_name': 'pharmacy_claim',
        'schema_suffix': 'core',
        'expression': 'COALESCE(SUM(paid_amount), 0)'
    }
]) %}

{% if metric_definitions | length == 0 %}
select cast(null as varchar(1)) as version,
       cast(null as varchar(1)) as metric_name,
       cast(null as varchar(1)) as mart,
       cast(null as varchar(1)) as table_name,
       cast(null as numeric) as metric_value,
       cast(null as varchar(1)) as tuva_last_run,
       cast(null as varchar(1)) as base_schema_tuva_last_run,
       cast(null as varchar(1)) as head_schema_tuva_last_run
where 1 = 0
{% else %}
{% set versions = [
    {'name': 'base', 'prefix': base_prefix},
    {'name': 'head', 'prefix': head_prefix}
] %}

{% set run_anchor_relations = {} %}
{% for version in versions %}
    {% set version_schema_prefix = version.prefix %}
    {% set anchor = namespace(relation=none) %}

    {# Prefer core tables if present #}
    {% set core_candidates = ['medical_claim', 'encounter', 'member_months', 'pharmacy_claim', 'person_id_crosswalk'] %}
    {% for identifier in core_candidates %}
        {% set candidate = adapter.get_relation(database=database_name, schema=version_schema_prefix ~ 'core', identifier=identifier) %}
        {% if candidate is not none %}
            {% set anchor.relation = candidate %}
            {% break %}
        {% endif %}
    {% endfor %}

    {# Otherwise, pick the first available final model relation #}
    {% if anchor.relation is none %}
        {% for node in final_nodes %}
            {% set candidate = adapter.get_relation(database=database_name, schema=version_schema_prefix ~ node.mart, identifier=node.alias) %}
            {% if candidate is not none %}
                {% set anchor.relation = candidate %}
                {% break %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% do run_anchor_relations.update({version.name: anchor.relation}) %}
{% endfor %}

with
{% for version in versions %}
    {% set version_schema_prefix = version.prefix %}
    {% set version_name = version.name %}
{{ version_name }}_metrics as (
    {% for metric in metric_definitions %}
        {% set schema_name = version_schema_prefix ~ metric.schema_suffix %}
        {% set relation = adapter.get_relation(database=database_name, schema=schema_name, identifier=metric.table_name) %}
    select '{{ version_name }}' as version,
           '{{ metric.metric_name }}' as metric_name,
           '{{ metric.mart }}' as mart,
           '{{ metric.table_name }}' as table_name,
        {% if relation is none %}
           0 as metric_value,
        {% else %}
           {{ metric.expression }} as metric_value,
        {% endif %}
           '{{ var('tuva_last_run') }}' as tuva_last_run
        {% if relation is not none %}
    from {{ relation }}
        {% endif %}
        {% if not loop.last %}
    union all
        {% endif %}
    {% endfor %}
)
    {% if not loop.last %}
,
    {% endif %}
{% endfor %},
run_timestamps as (
    select
        {% for version in versions %}
            {% set anchor_relation = run_anchor_relations[version.name] %}
            {% if anchor_relation is none %}
                cast(null as {{ dbt.type_string() }}) as {{ version.name }}_schema_tuva_last_run
            {% else %}
                cast((select max(tuva_last_run) from {{ anchor_relation }}) as {{ dbt.type_string() }}) as {{ version.name }}_schema_tuva_last_run
            {% endif %}
            {% if not loop.last %},{% endif %}
        {% endfor %}
)
select version,
       metric_name,
       mart,
       table_name,
       metric_value,
       tuva_last_run,
       run_timestamps.base_schema_tuva_last_run,
       run_timestamps.head_schema_tuva_last_run
from base_metrics
cross join run_timestamps

union all

select version,
       metric_name,
       mart,
       table_name,
       metric_value,
       tuva_last_run,
       run_timestamps.base_schema_tuva_last_run,
       run_timestamps.head_schema_tuva_last_run
from head_metrics
cross join run_timestamps
{% endif %}
