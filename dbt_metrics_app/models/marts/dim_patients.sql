with source as (
    select * from {{ source('raw','raw_patients') }}
),

final as (
    select * from source
)


select * from final