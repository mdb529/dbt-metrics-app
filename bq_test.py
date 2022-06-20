from google.cloud import bigquery
from google.oauth2 import service_account
from matplotlib.pyplot import grid
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from time import sleep
print('BEGIN...')

pd.set_option('display.float_format',lambda x: '%.f' % x)

# df = client.query(QUERY).to_dataframe()
# df.to_pickle('sample_data.pkl')

df = pd.read_pickle('sample_data.pkl')

grid_df = df[['period','practice','net_charges']].sort_values(by='period',ascending=False)
grid_df.net_charges = grid_df.net_charges.astype('float64')
grid_df.period = grid_df.period.astype('datetime64')
print(grid_df.head())
print(grid_df.info())
