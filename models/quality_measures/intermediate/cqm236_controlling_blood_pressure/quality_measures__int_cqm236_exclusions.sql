{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(
  select 
    performance_period_begin
  from {{ ref('quality_measures__int_cqm236__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(
  select 
    performance_period_end
  from {{ ref('quality_measures__int_cqm236__performance_period') }}

)
{%- endset -%}

-- First, get the performance period values only once
with frailty as (
  select
      patient_id,
      exclusion_date,
      exclusion_reason
  from {{ ref('quality_measures__int_shared_exclusions_frailty') }}
  where exclusion_date between {{ performance_period_begin }}
    and {{ performance_period_end }}
),

denominator as (
  select distinct
      patient_id,
      age
  from {{ref('quality_measures__int_cqm236_denominator')}}
),

advanced_illness_exclusion as (
  select
    source.patient_id,
    source.exclusion_date,
    source.exclusion_reason,
    source.exclusion_type
  from {{ ref('quality_measures__int_shared_exclusions_advanced_illness') }} as source
  inner join frailty
    on source.patient_id = frailty.patient_id
  where source.exclusion_date
    between
      {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp=performance_period_begin) }}
      and {{ performance_period_end }}
),

acute_inpatient_advanced_illness as (
  select *
  from advanced_illness_exclusion
  where exclusion_type = 'acute_inpatient'
),

nonacute_outpatient_advanced_illness as (
  select *
  from advanced_illness_exclusion
  where exclusion_type = 'nonacute_outpatient'
),

-- Combine inpatient and outpatient counts logic into a single CTE to reduce duplication
advanced_illness_counts as (
  select
      patient_id,
      exclusion_type,
      count(distinct exclusion_date) as encounter_count
  from advanced_illness_exclusion
  group by patient_id, exclusion_type
),

valid_advanced_illness_exclusions as (
  select
      a.patient_id,
      a.exclusion_date,
      a.exclusion_reason,
      a.exclusion_type    
  from advanced_illness_exclusion a
  inner join advanced_illness_counts c
    on a.patient_id = c.patient_id
  where (a.exclusion_type = 'acute_inpatient' and c.encounter_count >= 1)
     or (a.exclusion_type = 'nonacute_outpatient' and c.encounter_count >= 2)
),

valid_exclusions as (
  select 
      patient_id,
      exclusion_date,
      exclusion_reason,
      exclusion_type
  from valid_advanced_illness_exclusions
  union all
  select 
      patient_id,
      exclusion_date,
      exclusion_reason,
      exclusion_type
  from {{ref('quality_measures__int_shared_exclusions_dementia')}}
  where exclusion_date between 
    {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp= performance_period_begin ) }}
          and {{ performance_period_end }}
  union all
  select 
      patient_id,
      exclusion_date,
      exclusion_reason,
      exclusion_type
  from {{ref('quality_measures__int_shared_exclusions_hospice_palliative')}}
  union all
  select 
      patient_id,
      exclusion_date,
      exclusion_reason,
      exclusion_type
  from {{ref('quality_measures__int_shared_exclusions_institutional_snp')}}
),

combined_exclusions as (
  select 
      e.patient_id,
      e.exclusion_date,
      e.exclusion_reason,
      e.exclusion_type,
      d.age
  from valid_exclusions e
  inner join denominator d
      on e.patient_id = d.patient_id
),

frailty_exclusion_older_than_80 as (
  select
      f.patient_id,
      f.exclusion_date,
      f.exclusion_reason,
      'measure specific exclusion for patients older than 80' as exclusion_type,
      d.age
  from frailty f
  inner join denominator d
      on f.patient_id = d.patient_id
  where d.age >= 81
),

measure_specific_procedure_observation_exclusion as (
  select
      patient_id,
      exclusion_date,
      exclusion_reason,
      exclusion_type,
      age
  from {{ref('quality_measures__int_cqm236_exclude_procedures_observations')}}
),

valid_exclusions_combined as (
  select 
      patient_id,
      exclusion_date,
      exclusion_reason,
      exclusion_type,
      age
  from combined_exclusions
  where exclusion_type != 'hospice_palliative' and age >= 66
  union all
  select 
      patient_id,
      exclusion_date,
      exclusion_reason,
      exclusion_type,
      age
  from combined_exclusions
  where exclusion_type = 'hospice_palliative'
  union all
  select 
      patient_id,
      exclusion_date,
      exclusion_reason,
      exclusion_type,
      age
  from frailty_exclusion_older_than_80
  union all
  select 
      patient_id,
      exclusion_date,
      exclusion_reason,
      exclusion_type,
      age
  from measure_specific_procedure_observation_exclusion
),

add_data_types as (
  select
      distinct
      cast(patient_id as TEXT) as patient_id,
      cast(exclusion_date as DATE) as exclusion_date,
      cast(exclusion_reason as TEXT) as exclusion_reason,
      1 as exclusion_flag
  from valid_exclusions_combined
)

select
    patient_id,
    exclusion_date,
    exclusion_reason,
    exclusion_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
-- where False