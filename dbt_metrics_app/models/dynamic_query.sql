
        select * from
        {{ 
            metrics.metric(
                metric_name='net_charges',
                grain='day',
                dimensions=[],
                secondary_calculations=[]
            )
        }}
        where period between date('2022-01-01') and date('2022-06-01')
        order by period
        