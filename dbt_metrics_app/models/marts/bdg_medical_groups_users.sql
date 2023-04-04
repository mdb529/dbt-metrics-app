with source as (
    select * from {{ source('raw','raw_medical_groups_users') }}
),

final as (
    select * from source
)


select * from final