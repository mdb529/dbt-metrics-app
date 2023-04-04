with source as (
    select * from {{ source('raw','raw_patient_access_log') }}
),

final as (
    select * from source
)


select * from final