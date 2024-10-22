{{ config(
    enabled = var('claims_enabled', False)
) }}

SELECT DISTINCT -- to bring to claim_ID grain
      m.data_source
    , coalesce(cast(m.paid_date as {{ dbt.type_string() }}),cast('1900-01-01' as {{ dbt.type_string() }})) as source_date
    , 'PHARMACY_CLAIM' AS table_name
    , 'Claim ID | Claim Line Number' AS drill_down_key
    , {{ dbt.concat(["coalesce(cast(m.claim_id as " ~ dbt.type_string() ~ "), 'null')",
                    "'|'",
                    "coalesce(cast(m.claim_line_number as " ~ dbt.type_string() ~ "), 'null')"]) }} as drill_down_value
    , 'PHARMACY' AS claim_type
    , 'NDC_CODE' AS field_name
    , case when term.ndc is not null          then        'valid'
          when m.ndc_code is not null        then 'invalid'
                                             else 'null' end as bucket_name
    , case
        when m.ndc_code is not null
            and term.ndc is null
            then 'NDC Code does not join to Terminology NDC table'
        else null
    end as invalid_reason
    , {{ dbt.concat(["m.ndc_code", "'|'", "coalesce(term.rxnorm_description, term.fda_description, '')"]) }} as field_value
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('pharmacy_claim')}} m
left join {{ ref('terminology__ndc')}} as term on m.ndc_code = term.ndc