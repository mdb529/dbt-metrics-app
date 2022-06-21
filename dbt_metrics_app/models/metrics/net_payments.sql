with payments as (
    select * 
from {{ metrics.metric(
    metric_name='net_payments',
    grain='week',
    dimensions=['practice','facility','was_claim_worked'],
    secondary_calculations=[

        metrics.period_to_date(aggregate="average", period="year", alias="payments_this_year_average"),

        metrics.rolling(aggregate="average", interval=3, alias="payments_avg_past_3_weeks"),
        metrics.rolling(aggregate="average", interval=6, alias="payments_avg_past_6_weeks"),
        metrics.rolling(aggregate="average", interval=12, alias="payments_avg_past_12_weeks"),
        metrics.rolling(aggregate="average", interval=52, alias="payments_avg_past_52_weeks"),
        
        metrics.rolling(aggregate="max", interval=3, alias="payments_max_past_3_weeks"),
        metrics.rolling(aggregate="max", interval=6, alias="payments_max_past_6_weeks"),
        metrics.rolling(aggregate="max", interval=12, alias="payments_max_past_12_weeks"),

        metrics.rolling(aggregate="min", interval=3, alias="payments_min_past_3_weeks"),
        metrics.rolling(aggregate="min", interval=6, alias="payments_min_past_6_weeks"),
        metrics.rolling(aggregate="min", interval=12, alias="payments_min_past_12_weeks"),
        metrics.period_to_date(aggregate="sum", period="year", alias="payments_this_year_total")
    ],
    start_date='2022-01-01',
    end_date='2022-06-01'
) }}
)



select * from payments