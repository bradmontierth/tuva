{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with combine_header_models as (
  {{ dbt_utils.union_relations(
    relations=[
      ref('service_category__facility_hospice_professional'),
      ref('service_category__home_hospice_professional')
    ],
    exclude=["_loaded_at"]
  ) }}
)

select
  h.claim_id
  , h.data_source
  , h.service_category_1
  , h.service_category_2
  , h.service_category_3
  , h.source_model_name
from combine_header_models as h

