{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    s.claim_id
  , s.data_source
  , 'inpatient' as service_category_1
  , 'facility hospice' as service_category_2
  , 'facility hospice' as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as s
where s.claim_type = 'institutional'
  and s.hcpcs_code in ('Q5005', 'Q5006', 'Q5007', 'Q5008', 'Q5009', 'Q5010')
  and not exists (
    select 1
    from {{ ref('service_category__home_health_institutional') }} as hhi
    where s.claim_id = hhi.claim_id
      and s.data_source = hhi.data_source
  )
