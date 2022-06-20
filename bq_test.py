from google.cloud import bigquery
from google_auth_oauthlib import flow
from google.oauth2 import service_account
from matplotlib.pyplot import grid
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from time import sleep

print('BEGIN...')

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)



query_string = """
SELECT * FROM `dbt-metrics-dw.analytics.net_charges` LIMIT 1000
"""
query_job = client.query(query_string)

# Print the results.
for row in query_job.result():  # Wait for the job to complete.
    print("{}: {}".format(row["practice"], row["net_charges"]))



# pd.set_option('display.float_format',lambda x: '%.f' % x)

# df = client.query(QUERY).to_dataframe()
# df.to_pickle('sample_data.pkl')

##### TEST UPDATE

# df = pd.read_pickle('sample_data.pkl')

# grid_df = df[['period','practice','net_charges']].sort_values(by='period',ascending=False)
# grid_df.net_charges = grid_df.net_charges.astype('float64')
# grid_df.period = grid_df.period.astype('datetime64')
# print(grid_df.head())
# print(grid_df.info())


print('...END')
