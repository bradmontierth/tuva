{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    med.claim_id --claim level
  , med.data_source
  , 'outpatient' as service_category_1
  , 'outpatient surgery' as service_category_2
  , 'outpatient surgery' as service_category_3
  , '{{ this.name }}' as source_model_name
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} as med
inner join {{ ref('service_category__stg_outpatient_institutional') }} as o
  on med.claim_id = o.claim_id
  and med.data_source = o.data_source
where ccs_category between '1' and '176'
  or ccs_category in ('229', '230', '232', '244')
  or (
    ccs_category = '231'
    and hcpcs_code not in (
      '96360', '96361', '96365', '96366', '96367', '96368', '96369', '96370',
      '96371', '96372', '96373', '96374', '96375', '96376', '96379'
    )
  )
