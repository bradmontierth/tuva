{{ config(
     enabled = var('hcc_recapture_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))) | as_bool
}}

-- Flattening months to 1 person per year
select distinct
  m.person_id
  , {{ date_part('year', 'm.collection_end_date') }} as collection_year
  , p.payer
from {{ ref('cms_hcc__int_members') }} as m
inner join {{ ref('cms_hcc__patient_payer_monthly') }} as p
  on m.person_id = p.person_id
  and m.payment_year = p.payment_year
  and m.collection_end_date = p.collection_end_date
  and p.is_primary_payer = {% if target.type == 'fabric' %}cast(1 as bit){% else %}true{% endif %}
-- Don't support ESRD risk scores yet
where m.enrollment_status != 'ESRD'
