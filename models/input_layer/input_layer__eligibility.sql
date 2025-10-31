{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}
with src as (
  select *
  from {{ ref('eligibility') }}
)
select
  src.*
  {{ ensure_optional_column(ref('eligibility'), 'medicare_part_b_enrollment_start_date', 'date') }}
from src
