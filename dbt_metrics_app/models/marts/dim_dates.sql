{{
    config(
        materialized = "table"
    )
}}

with days as (
    
    {{ dbt_date.get_date_dimension('2020-01-01', '2022-12-31') }}

),

final as (
    select 
        *,
        cast({{ dbt_utils.date_trunc('week', 'date_day') }} as date) as date_week,
        cast({{ dbt_utils.date_trunc('month', 'date_day') }} as date) as date_month,
        cast({{ dbt_utils.date_trunc('quarter', 'date_day') }} as date) as date_quarter,
        cast({{ dbt_utils.date_trunc('year', 'date_day') }} as date) as date_year
    from days
)

select * from final