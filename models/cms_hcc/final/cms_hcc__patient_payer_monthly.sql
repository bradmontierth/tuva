{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

/*
  Payer breakout for CMS-HCC.

  Rationale: avoid changing the grain of CMS-HCC risk scoring outputs while still
  allowing users to join payer context when needed.

  Grain: person_id + payment_year + collection_end_date + payer
*/

with elig as (

    select
          person_id
        , payer
        , data_source
        , enrollment_start_date
        , enrollment_end_date
    from {{ ref('cms_hcc__stg_core__eligibility') }}

)

, months as (

    select
          payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_monthly_collection_dates') }}

)

, overlaps as (

    select distinct
          e.person_id
        , e.payer
        , e.data_source
        , m.payment_year
        , m.collection_start_date
        , m.collection_end_date
        , e.enrollment_start_date
        , e.enrollment_end_date
    from elig e
    inner join months m
        on e.enrollment_start_date <= m.collection_end_date
        and e.enrollment_end_date >= m.collection_start_date

)

, ranked as (

    select
          *
        , row_number() over (
            partition by person_id, payment_year, collection_end_date
            order by enrollment_end_date desc, payer
          ) as payer_rank
    from overlaps

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(payer_rank as integer) as payer_rank
        {% if target.type == 'fabric' %}
            , cast(case when payer_rank = 1 then 1 else 0 end as bit) as is_primary_payer
        {% else %}
            , cast(case when payer_rank = 1 then true else false end as boolean) as is_primary_payer
        {% endif %}
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from ranked

)

select
      person_id
    , payer
    , data_source
    , payer_rank
    , is_primary_payer
    , payment_year
    , collection_start_date
    , collection_end_date
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from add_data_types

