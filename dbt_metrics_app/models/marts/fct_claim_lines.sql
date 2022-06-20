with claim_lines as (
    select * from {{ source('raw','raw_claim_lines') }}
)

select * from claim_lines