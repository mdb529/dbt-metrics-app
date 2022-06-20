from re import A
from matplotlib.pyplot import grid
import matplotlib.pyplot as plt
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import numpy as np
import os
from utils import MetricsUtil
import plotly.express as px


header = st.container()

with header:
    st.title('Welcome to my data app!')
    st.text('In this app I am showcasing the dbt Metrics feature')

metrics = MetricsUtil()

DEBUG = st.sidebar.checkbox("Debug Mode", value=False)

selected_metric_name = st.sidebar.selectbox(
    label="Select a metric", options=sorted(list(metrics.get_metric_names().keys()))
)

selected_node_id = metrics.metrics_list[selected_metric_name]
node = metrics.manifest["metrics"][selected_node_id]
available_dimensions = node["dimensions"]
available_time_grains = node["time_grains"]


selected_dimension = st.sidebar.radio(
    "Select a dimension", options=["none"] + available_dimensions
)
selected_time_grain = st.sidebar.selectbox(
    "Select a time grain", options=available_time_grains
)

calculation_options = [
    'metrics.period_over_period(comparison_strategy="ratio", interval=1)',
    'metrics.period_over_period(comparison_strategy="difference", interval=1)',
    'metrics.rolling(aggregate="max", interval=1)',
    'metrics.rolling(aggregate="min", interval=1)',
]
secondary_calcs_list = st.sidebar.multiselect(
    "Select secondary calculations", options=calculation_options
)



def get_min_max_dates(metric_name):
    
    return "2022-01-01", "2022-06-01"


min_date, max_date = get_min_max_dates(selected_metric_name)

if selected_dimension == "none":
    selected_dimensions_list = []
else:
    selected_dimensions_list = [selected_dimension]

query = metrics.populate_template_query(
    metric_name=selected_metric_name,
    time_grain=selected_time_grain,
    dimensions_list=selected_dimensions_list,
    secondary_calcs_list="[" + ",".join(secondary_calcs_list) + "]",
    min_date=min_date,
    max_date=max_date,
)

with st.spinner("Fetching query results"):
    df = metrics.get_query_results(query)
    df.columns = [c.lower() for c in df.columns]


st.subheader(node["label"])
st.markdown(node["description"])
st.markdown(f'This metric is based on the model {node["model"]}. It is a {node["type"]} metric based on the column {node["sql"]}.')


if DEBUG:
    compiled = metrics._get_compiled_query(query)
    st.subheader("Compiled SQL")
    st.text(compiled)

with st.spinner("Plotting results"):
    if selected_dimension == "none" == "none":
        title=f"{selected_metric_name} over time"
        fig = px.line(df,x="period",y=selected_metric_name,title=title)
    else:
        title=f"{selected_metric_name} by {selected_dimension} over time"
        fig = px.line(df,x="period",y=selected_metric_name,color=selected_dimension,title=title)

    st.plotly_chart(fig)

st.subheader("Data Table")
st.dataframe(df)

st.subheader("dbt Query")
st.text(query)

