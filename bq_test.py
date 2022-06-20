from google.cloud import bigquery
from google.oauth2 import service_account
from matplotlib.pyplot import grid
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from time import sleep
print('BEGIN...')

'''
    # credentials = service_account.Credentials.from_service_account_file('/Users/michaeldouglas/bq-dbt-metrics-app-serviceaccount.json')
    # client = bigquery.Client(credentials= credentials,project='dbt-metrics-dw')

    # # Perform a query.
    # QUERY = (
    #     """
    #     with charges as (
    #     select * 
    #         from -- Need this here, since the actual ref is nested within loops/conditions:
    #             -- depends on: `dbt-metrics-dw`.`analytics`.`dbt_metrics_default_calendar`
    #             (with source_query as (

    #             select
    #                 /* Always trunc to the day, then use dimensions on calendar table to achieve the _actual_ desired aggregates. */
    #                 /* Need to cast as a date otherwise we get values like 2021-01-01 and 2021-01-01T00:00:00+00:00 that don't join :( */
    #                 cast(timestamp_trunc(
    #                 cast(cast(DOS as date) as timestamp),
    #                 day
    #             ) as date) as date_day,
                    
    #                 practice,
    #                     facility,
    #                     net_charges as property_to_aggregate

    #             from `dbt-metrics-dw`.`analytics`.`fct_claim_lines`
    #             where 1=1
    #         ),

    #         spine__time as (
    #             select 
    #                 /* this could be the same as date_day if grain is day. That's OK! 
    #                 They're used for different things: date_day for joining to the spine, period for aggregating.*/
    #                 date_week as period, 
                    
    #                     date_year,
                    
                    
    #                 date_day
    #             from `dbt-metrics-dw`.`analytics`.`dbt_metrics_default_calendar`

    #         ),


    #         spine__values as (
    #             select distinct
    #                         practice
    #                         ,
                        
    #                         facility
                            
                        
    #             from source_query

    #         ),  
    #         spine as (

    #             select *
    #             from spine__time
                
    #             cross join spine__values
    #             ),

    #         joined as (
    #             select 
    #                 spine.period,
                    
    #                 spine.date_year,
                    
                    
    #                 spine.practice,
                    
    #                 spine.facility,
                    

    #                 -- has to be aggregated in this CTE to allow dimensions coming from the calendar table
    #             sum(source_query.property_to_aggregate)
    #         as net_charges,
    #                 logical_or(source_query.date_day is not null) as has_data

    #             from spine
    #             left outer join source_query on source_query.date_day = spine.date_day
                
    #                     and (  source_query.practice = spine.practice
    #                         or source_query.practice is null and spine.practice is null
    #                     )
                
    #                     and (  source_query.facility = spine.facility
    #                         or source_query.facility is null and spine.facility is null
    #                     )
                
    #             group by 1, 2, 3, 4

    #         ),

    #         bounded as (
    #             select 
    #                 *,
    #                 cast('2022-01-01' as date) as lower_bound,
    #                 cast('2022-06-01' as date) as upper_bound
    #             from joined 
    #         ),

    #         secondary_calculations as (

    #             select *
                    
    #                 , 
    #             avg(net_charges)
    #         over (
    #                     partition by date_year
    #                     , practice, facility
    #                     order by period
    #                     rows between unbounded preceding and current row
    #                 )

    #         as charges_this_year_average

    #                 , 
                    
    #             avg(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 2 preceding and current row
    #                 )
                

    #         as charges_avg_past_3_weeks

    #                 , 
                    
    #             avg(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 5 preceding and current row
    #                 )
                

    #         as charges_avg_past_6_weeks

    #                 , 
                    
    #             avg(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 11 preceding and current row
    #                 )
                

    #         as charges_avg_past_12_weeks

    #                 , 
                    
    #             avg(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 51 preceding and current row
    #                 )
                

    #         as charges_avg_past_52_weeks

    #                 , 
                    
    #             max(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 2 preceding and current row
    #                 )
                

    #         as charges_max_past_3_weeks

    #                 , 
                    
    #             max(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 5 preceding and current row
    #                 )
                

    #         as charges_max_past_6_weeks

    #                 , 
                    
    #             max(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 11 preceding and current row
    #                 )
                

    #         as charges_max_past_12_weeks

    #                 , 
                    
    #             min(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 2 preceding and current row
    #                 )
                

    #         as charges_min_past_3_weeks

    #                 , 
                    
    #             min(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 5 preceding and current row
    #                 )
                

    #         as charges_min_past_6_weeks

    #                 , 
                    
    #             min(net_charges)

    #                 over (
    #                     partition by practice, facility 
    #                     order by period
    #                     rows between 11 preceding and current row
    #                 )
                

    #         as charges_min_past_12_weeks

    #                 , 
    #             sum(net_charges)
    #         over (
    #                     partition by date_year
    #                     , practice, facility
    #                     order by period
    #                     rows between unbounded preceding and current row
    #                 )

    #         as charges_this_year_total

                    

    #             from bounded
                
    #         ),

    #         final as (
    #             select
    #                 period
                    
    #                 , practice
                    
    #                 , facility
                    
    #                 , coalesce(net_charges, 0) as net_charges
                    
    #                 , charges_this_year_average
                    
    #                 , charges_avg_past_3_weeks
                    
    #                 , charges_avg_past_6_weeks
                    
    #                 , charges_avg_past_12_weeks
                    
    #                 , charges_avg_past_52_weeks
                    
    #                 , charges_max_past_3_weeks
                    
    #                 , charges_max_past_6_weeks
                    
    #                 , charges_max_past_12_weeks
                    
    #                 , charges_min_past_3_weeks
                    
    #                 , charges_min_past_6_weeks
                    
    #                 , charges_min_past_12_weeks
                    
    #                 , charges_this_year_total
                    

    #             from secondary_calculations
    #             where period >= lower_bound
    #             and period <= upper_bound
    #             order by 1, 2, 3
    #         )

    #         select * from final

    #         ) metric_subq
    #         )



    #     select * from charges
    #     """
    # )
'''

pd.set_option('display.float_format',lambda x: '%.f' % x)

# df = client.query(QUERY).to_dataframe()
# df.to_pickle('sample_data.pkl')

df = pd.read_pickle('sample_data.pkl')

grid_df = df[['period','practice','net_charges']].sort_values(by='period',ascending=False)
grid_df.net_charges = grid_df.net_charges.astype('float64')
grid_df.period = grid_df.period.astype('datetime64')
print(grid_df.head())
print(grid_df.info())
