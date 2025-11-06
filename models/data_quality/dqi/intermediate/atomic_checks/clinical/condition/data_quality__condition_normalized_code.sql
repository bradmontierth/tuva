{{ config(
    enabled = var('clinical_enabled', False)
) }}

with icd10_release_year as (
  select
    max(release_year) as max_release_year
  from {{ ref('terminology__icd_10_cm_scd') }}
)

select
      m.data_source
    , coalesce(m.recorded_date,cast('1900-01-01' as date)) as source_date
    , 'CONDITION' as table_name
    , 'Condition ID' as drill_down_key
    , coalesce(condition_id, 'NULL') as drill_down_value
    , 'NORMALIZED_CODE' as field_name
    , case when term.icd_10_cm is not null then 'valid'
          when m.normalized_code is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.normalized_code is not null and term.icd_10_cm is null
           then 'Normalized code does not join to Terminology icd_10_cm table'
           else null end as invalid_reason
    , cast(normalized_code as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('condition') }} as m
cross join icd10_release_year as i10ry
left outer join {{ ref('terminology__icd_10_cm_scd') }} as term
    on m.normalized_code = term.icd_10_cm
    and {{ apply_icd10_valid_date_filter("m.recorded_date", 'term', 'i10ry') }}
