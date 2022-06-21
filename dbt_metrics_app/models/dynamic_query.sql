
        select * from
        {{ 
            metrics.metric(
                metric_name='net_payments',
                grain='day',
                dimensions=[],
                secondary_calculations=[metrics.rolling(aggregate="min", interval=3, alias="charges_min_past_3_weeks")]
            )
        }}
        where period between date('2022-01-01') and date('2022-06-01')
        order by period
        