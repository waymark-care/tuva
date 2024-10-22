{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain 
    m.data_source
    ,coalesce(cast(m.claim_start_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    ,'MEDICAL_CLAIM' AS table_name
    ,'Claim ID' as drill_down_key
    ,coalesce(m.claim_id, 'NULL') AS drill_down_value
    ,m.claim_type as claim_type
    ,'DIAGNOSIS_CODE_TYPE' AS field_name
    ,case when m.diagnosis_code_type is null then 'null'
          when term.code_type is null then 'invalid'
                             else 'valid' end as bucket_name
    ,case
        when m.diagnosis_code_type is not null and term.code_type is null then 'Diagnosis Code Type does not join to Terminology Code Type table'
        else null
    end as invalid_reason
    ,cast(m.diagnosis_code_type as {{ dbt.type_string() }}) as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim')}} m
left join {{ ref('reference_data__code_type')}} term on m.diagnosis_code_type = term.code_type