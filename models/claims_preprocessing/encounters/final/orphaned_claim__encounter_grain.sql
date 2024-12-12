{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with encounter_date as (
    select
        cli.encounter_id
      , min(stg.start_date) as encounter_start_date
      , max(stg.start_date) as encounter_end_date
    from {{ ref('encounters__stg_medical_claim') }} as stg
    inner join {{ ref('encounters__orphaned_claims') }} as cli
      on stg.claim_id = cli.claim_id
      and stg.claim_line_number = cli.claim_line_number
    group by
        cli.encounter_id
)

,detail_values as (
    select stg.*
    ,cli.encounter_id
    ,cli.encounter_type
    ,cli.encounter_group
    ,d.encounter_start_date 
    ,d.encounter_end_date
    , row_number() over (partition by cli.encounter_id order by stg.claim_type, stg.start_date) as encounter_row_number --institutional then professional
    from  {{ ref('encounters__stg_medical_claim') }} stg
    inner join {{ ref('encounters__orphaned_claims') }} cli on stg.claim_id = cli.claim_id  
    and
    stg.claim_line_number = cli.claim_line_number
    inner join encounter_date d on cli.encounter_id = d.encounter_id
)

, patient as (
    select 
        patient_data_source_id
        , birth_date
        , gender
        , race
    from {{ ref('encounters__stg_eligibility') }}
    where patient_row_num = 1
    )

, total_amounts as (
    select 
    encounter_id
    , encounter_type
    , encounter_group
    , sum(paid_amount) as total_paid_amount
    , sum(allowed_amount) as total_allowed_amount
    , sum(charge_amount) as total_charge_amount
    , count(distinct claim_id) as claim_count
    , count(distinct(case when claim_type = 'institutional' then claim_id else null end))  as inst_claim_count
    , count(distinct(case when claim_type = 'professional' then claim_id else null end))  as prof_claim_count
from detail_values
group by encounter_id
,encounter_type 
,encounter_group
)


,highest_paid_diagnosis as 
(
  select encounter_id
  , diagnosis_code_1
  , diagnosis_code_type
  , row_number() over (partition by encounter_id order by sum(paid_amount) desc ) as paid_order
  , sum(paid_amount) as paid_amount
  from detail_values
  where diagnosis_code_1 is not null
  group by diagnosis_code_1
  , encounter_id
  , diagnosis_code_type
)

,highest_paid_facility as 
(
  select encounter_id
  , facility_id
  , row_number() over (partition by encounter_id order by sum(paid_amount) desc ) as paid_order
  , sum(paid_amount) as paid_amount
  from detail_values
  where facility_id is not null
  group by 
   encounter_id
  , facility_id
)

,highest_paid_pos as 
(
  select encounter_id
  , place_of_service_code
  , place_of_service_description
  , row_number() over (partition by encounter_id order by sum(paid_amount) desc ) as paid_order
  , sum(paid_amount) as paid_amount
  from detail_values
  where place_of_service_code is not null
  group by 
   encounter_id
  , place_of_service_code
  , place_of_service_description
)



, service_category_flags as (
    select distinct
        d.encounter_id
       ,max(case when scr.service_category_2 = 'lab' then 1 else 0 end) as lab_flag
       ,max(case when scr.service_category_2 = 'ambulance' then 1 else 0 end) as ambulance_flag
       ,max(case when scr.service_category_2 = 'durable medical equipment' then 1 else 0 end) as dme_flag
       ,max(case when scr.service_category_2 = 'observation' then 1 else 0 end) as observation_flag
       ,max(case when scr.service_category_2 = 'pharmacy' then 1 
              else 0 end) as pharmacy_flag
    from detail_values d
    left join {{ ref('service_category__service_category_grouper') }} scr on d.claim_id = scr.claim_id 
    and
    scr.claim_line_number = d.claim_line_number
    group by d.encounter_id
)

select   d.encounter_id
, d.encounter_start_date
, d.encounter_end_date
, d.patient_data_source_id
,tot.encounter_type
,tot.encounter_group
, {{ dbt.datediff("birth_date","d.encounter_start_date","day")}}/365 as admit_age
, e.gender
, e.race
, hp.diagnosis_code_type as primary_diagnosis_code_type
, hp.diagnosis_code_1 as primary_diagnosis_code
, coalesce(icd10cm.long_description, icd9cm.long_description) as primary_diagnosis_description
, hf.facility_id as facility_id
, b.provider_organization_name as facility_name
, b.primary_specialty_description as facility_type
, sc.lab_flag
, sc.dme_flag
, sc.ambulance_flag
, sc.pharmacy_flag
, pos.place_of_service_code
, pos.place_of_service_description
, tot.total_paid_amount
, tot.total_allowed_amount
, tot.total_charge_amount
, tot.claim_count
, tot.inst_claim_count
, tot.prof_claim_count
, d.data_source
, '{{ var('tuva_last_run')}}' as tuva_last_run
from detail_values d
inner join total_amounts tot on d.encounter_id = tot.encounter_id
inner join service_category_flags sc on d.encounter_id = sc.encounter_id
left join highest_paid_diagnosis hp on d.encounter_id = hp.encounter_id
and
hp.paid_order = 1
left join highest_paid_facility hf on d.encounter_id = hf.encounter_id
and
hf.paid_order = 1
left join highest_paid_pos pos on d.encounter_id = pos.encounter_id
and
pos.paid_order = 1
left join patient e
  on d.patient_data_source_id = e.patient_data_source_id
left join {{ ref('terminology__provider') }} b
  on hf.facility_id = b.npi
left join {{ ref('terminology__icd_10_cm') }} icd10cm
  on hp.diagnosis_code_1 = icd10cm.icd_10_cm
  and hp.diagnosis_code_type = 'icd-10-cm'
left join {{ ref('terminology__icd_9_cm') }} icd9cm
  on hp.diagnosis_code_1 = icd9cm.icd_9_cm
  and hp.diagnosis_code_type = 'icd-9-cm'
where d.encounter_row_number = 1