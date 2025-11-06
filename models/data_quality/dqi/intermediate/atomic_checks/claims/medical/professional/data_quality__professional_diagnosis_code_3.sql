{{ config(
    enabled = var('claims_enabled', False)
) }}

with icd10_release_year as (
  select
    max(release_year) as max_release_year
  from {{ ref('terminology__icd_10_cm_scd') }}
)

, base as (
    select *
    from {{ ref('medical_claim') }}
    where claim_type = 'professional'
)

select
      m.data_source
    , coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'MEDICAL_CLAIM' as table_name
    , 'Claim ID | Claim Line Number' as drill_down_key
    , {{ concat_custom(["coalesce(cast(m.claim_id as " ~ dbt.type_string() ~ "), 'null')",
                    "'|'",
                    "coalesce(cast(m.claim_line_number as " ~ dbt.type_string() ~ "), 'null')"]) }} as drill_down_value
    , 'professional' as claim_type
    , 'DIAGNOSIS_CODE_3' as field_name
    , case when term.icd_10_cm is not null          then 'valid'
          when m.diagnosis_code_3 is not null      then 'invalid'
                                                   else 'null' end as bucket_name
    , case
        when m.diagnosis_code_3 is not null
            and term.icd_10_cm is null
            then 'Diagnosis Code does not join to Terminology ICD_10_CM table'
        else null
    end as invalid_reason
    , {{ concat_custom(["m.diagnosis_code_3", "'|'", "coalesce(term.short_description, '')"]) }} as field_value
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from base as m
cross join icd10_release_year as i10ry
left outer join {{ ref('terminology__icd_10_cm_scd') }} as term
    on m.diagnosis_code_3 = term.icd_10_cm
    and {{ apply_icd10_valid_date_filter(
        "coalesce(" ~ try_to_cast_date('m.claim_line_start_date', 'YYYY-MM-DD') ~ ", " ~ try_to_cast_date('m.claim_start_date', 'YYYY-MM-DD') ~ ")",
        'term',
        'i10ry'
    ) }}
