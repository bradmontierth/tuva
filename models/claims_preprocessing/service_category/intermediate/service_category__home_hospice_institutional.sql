{{ config(
     enabled = var('claims_preprocessing_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

select distinct
    med.claim_id
  , med.data_source
  , 'outpatient' as service_category_1
  , 'home hospice' as service_category_2
  , 'home hospice' as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
where
  med.claim_type = 'institutional'
  and med.hcpcs_code in ('Q5001', 'Q5002', 'Q5003', 'Q5004')
  and not exists (
    select 1
    from {{ ref('service_category__stg_medical_claim') }} as fac
    where fac.claim_id = med.claim_id
      and fac.data_source = med.data_source
      and fac.claim_type = med.claim_type
      and fac.hcpcs_code in ('Q5005', 'Q5006', 'Q5007', 'Q5008', 'Q5009', 'Q5010')
  )

