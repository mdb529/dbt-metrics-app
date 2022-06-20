import os
from google.cloud import bigquery
from google.oauth2 import service_account

os.environ["BQ_SA_CREDENTIALS"] = "service_account.Credentials.from_service_account_file('/Users/michaeldouglas/bq-dbt-metrics-app-serviceaccount.json')"
os.environ["BQ_PROJECT"] = 'dbt-metrics-dw'

print(os.environ["BQ_SA_CREDENTIALS"])