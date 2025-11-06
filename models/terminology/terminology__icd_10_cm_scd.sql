{{ config(
    materialized = 'view',
    schema = (
        var('tuva_schema_prefix', none) ~ '_terminology'
        if var('tuva_schema_prefix', none) is not none
        else 'terminology'
    ),
    alias = 'icd_10_cm_scd',
    tags = ['terminology']
) }}

select
    SN,
    icd_10_cm,
    header_flag,
    short_description,
    long_description,
    release_year,
    start_valid_date,
    end_valid_date
from {{ source('lakehouse_terminology', 'terminology__icd_10_cm_scd') }}
