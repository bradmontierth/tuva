/* This model unions together claim lines to encounter crosswalk, and assigns them a unqiue encounter type if claims were assigned to multiple encounters
(This can happen a few ways - professional claims assigned to anchor event overlap and assigned to multiple  )

Note: `anchor_claim_id` is populated only for encounter types where an "anchor claim" concept exists (currently ED); it is used only as a tie-breaker in
`claim_line_attribution_number` ordering (prefer the row where `claim_id = anchor_claim_id`). This ensures the claims that triggered the event (anchor) 
ends up in the encounter after all deduping occurs.
*/

{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with cte as (
select claim_id
 , claim_line_number
 , data_source
 , encounter_id
 , 'acute inpatient' as encounter_type
 , 'inpatient' as encounter_group
 , null as priority_number
, null as anchor_claim_id
from {{ ref('acute_inpatient__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, med.data_source
, enc.encounter_id
, 'acute inpatient' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('acute_inpatient__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med
  on enc.claim_id = med.claim_id
  and enc.patient_data_source_id = med.patient_data_source_id

union all

/* Intentionally bringing in professional claims assigned to inpatient stays in case admit is assigned to ED  */
select claim_id
 , claim_line_number
 , data_source
 , encounter_id
 , 'emergency department' as encounter_type
 , 'outpatient' as encounter_group
 , null as priority_number
, null as anchor_claim_id
from {{ ref('acute_inpatient__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, med.data_source
, enc.encounter_id
, 'emergency department' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, original_anchor_claim as anchor_claim_id
from {{ ref('emergency_department__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med
  on enc.claim_id = med.claim_id
  and enc.patient_data_source_id = med.patient_data_source_id

union all

select claim_id
 , claim_line_number
 , data_source
 , encounter_id
 , 'emergency department' as encounter_type
 , 'outpatient' as encounter_group
 , null as priority_number
, null as anchor_claim_id
from {{ ref('emergency_department__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, med.data_source
, enc.encounter_id
, 'facility hospice' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('facility_hospice__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med
  on enc.claim_id = med.claim_id
  and enc.patient_data_source_id = med.patient_data_source_id

union all

select claim_id
, claim_line_number
 , data_source
, encounter_id
, 'inpatient psych' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_psych__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, med.data_source
, enc.encounter_id
, 'inpatient psych' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_psych__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med
  on enc.claim_id = med.claim_id
  and enc.patient_data_source_id = med.patient_data_source_id

union all

select claim_id
, claim_line_number
 , data_source
, encounter_id
, 'inpatient rehabilitation' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_rehab__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, med.data_source
, enc.encounter_id
, 'inpatient rehabilitation' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_rehab__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med
  on enc.claim_id = med.claim_id
  and enc.patient_data_source_id = med.patient_data_source_id

union all

select claim_id
, claim_line_number
, data_source
, encounter_id
, 'inpatient long term acute care' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_long_term__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, med.data_source
, enc.encounter_id
, 'inpatient long term acute care' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_long_term__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med
  on enc.claim_id = med.claim_id
  and enc.patient_data_source_id = med.patient_data_source_id


union all

select claim_id
, claim_line_number
, data_source
, encounter_id
, 'inpatient skilled nursing' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_snf__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, med.data_source
, enc.encounter_id
, 'inpatient skilled nursing' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_snf__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med
  on enc.claim_id = med.claim_id
  and enc.patient_data_source_id = med.patient_data_source_id

union all

select claim_id
, claim_line_number
, data_source
, encounter_id
, 'inpatient substance use' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_substance_use__prof_claims') }}
where claim_attribution_number = 1

union all

select enc.claim_id
, med.claim_line_number
, med.data_source
, enc.encounter_id
, 'inpatient substance use' as encounter_type
, 'inpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('inpatient_substance_use__generate_encounter_id') }} as enc
inner join {{ ref('encounters__stg_medical_claim') }} as med
  on enc.claim_id = med.claim_id
  and enc.patient_data_source_id = med.patient_data_source_id

union all

/* Priority of sub office based types from office based group are set within office_visits__int_office_visits_union model */
select claim_id
, claim_line_number
, data_source
, old_encounter_id
, encounter_type
, 'office based' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('office_visits__int_office_visits_claim_line') }}
where encounter_type = 'office visit radiology'


union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, encounter_type
, 'office based' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('office_visits__int_office_visits_claim_line') }}
where encounter_type <> 'office visit radiology'

union all

/* urgent care set at lower priority than ed and inpatient to avoid over flagging urgent care due to variations in billing practices */
select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'urgent care' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('urgent_care__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient psych' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_psych__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient rehabilitation' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_rehab__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'ambulatory surgery center' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('asc__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'dialysis' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('dialysis__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'home hospice' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('home_hospice__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'home health' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('home_health__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient surgery' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_surgery__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient injections' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_injections__match_claims_to_anchor') }}

union all


select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient pt/ot/st' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_ptotst__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient substance use' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_substance_use__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient radiology' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_radiology__match_claims_to_anchor') }}

union all

/* Set as lowest outpatient priority "catch all", roll up to more specific encounter type when available */
select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'outpatient hospital or clinic' as encounter_type
, 'outpatient' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('outpatient_hospital_or_clinic__match_claims_to_anchor') }}

union all

/* orphaned encounters are "last resort". Labs/DME/ambulance should roll up to inpatient/home health/etc. If unable to match, then they get their own encounter*/

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'lab - orphaned' as encounter_type
, 'other' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('lab__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'dme - orphaned' as encounter_type
, 'other' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('dme__match_claims_to_anchor') }}

union all

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'ambulance - orphaned' as encounter_type
, 'other' as encounter_group
, null as priority_number
, null as anchor_claim_id
from {{ ref('ambulance__match_claims_to_anchor') }}

)

 , encounter_priority as (
  select
      encounter_type
    , priority_number
  from {{ ref('encounters__encounter_type_priority') }}
 )

 , prioritized as (
  select
      c.claim_id
    , c.claim_line_number
    , c.data_source
    , c.encounter_id
    , c.encounter_type
    , c.encounter_group
    , coalesce(p.priority_number, 9999999) as priority_number
    , c.anchor_claim_id
  from cte as c
  left join encounter_priority as p
    on c.encounter_type = p.encounter_type
 )

select
  c.claim_id
, c.claim_line_number
, c.data_source
, c.encounter_id as old_encounter_id
, dense_rank() over (
order by c.data_source, c.encounter_type, c.encounter_id) as encounter_id
, c.encounter_type
, c.encounter_group
, c.priority_number
, c.anchor_claim_id
, row_number() over (
partition by c.claim_id, c.claim_line_number, c.data_source
order by
    c.priority_number
  , case when c.claim_id = c.anchor_claim_id then 1 else 99 end
  , c.encounter_type
  , c.encounter_id
) as claim_line_attribution_number
from prioritized as c
