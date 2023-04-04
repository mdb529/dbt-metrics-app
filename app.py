from re import A
from matplotlib.pyplot import grid
import matplotlib.pyplot as plt
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
import numpy as np
import os
from utils import MetricsUtil
import plotly.express as px
from st_on_hover_tabs import on_hover_tabs


st.set_page_config(layout="wide")


metrics = MetricsUtil()




st.markdown('<style>' + open('./style.css').read() + '</style>', unsafe_allow_html=True)


with st.sidebar:
    tabs = on_hover_tabs(tabName=['Overview', 'dbt Sources & Models', 'dbt Metrics'],iconName=['assignment','monitor_heart','dashboard'], default_choice=0)

if tabs =='Overview':
    st.title('Welcome to my data app!')
    st.text("This is a demo web app built using Streamlit. It showcases the power of dbt Metrics, something that was released by dbt Labs in late 2022.")
    st.text("On the backend, I used dbt to build a series of models in Google BigQuery.")
    st.text("Next, I created a series of metrics on top of these models, which you can explore in the 'Example' page")
    st.text("<-- See dbt Metrics in action!")
    st.caption("If on mobile, first click the '>' icon at the top left of your screen to expand the side menu and then click the 'Example' tab, otherwise click the 'Example' tab to see a demo")

elif tabs == 'dbt Sources & Models':
    st.title("dbt Sources & Models")
    st.text("These are the sources and models that were built using dbt. Metrics were built on top of these models.")

    col1, col2, col3 = st.columns(3)

    selected_node_type = col1.selectbox(
     label="Select a Node Type", options=['Sources','Models']
    )

    selected_node_name = ''
    query = ''
    if selected_node_type == 'Sources':
        selected_node_name = col2.selectbox(
        label="Select a Source", options=sorted(list(metrics.get_source_names().values()))
        )

        query = """
        select * from dbt-metrics-dw.raw.{} limit 50
        """.format(selected_node_name)

    elif selected_node_type == 'Models':
        selected_node_name = col2.selectbox(
        label="Select a Model", options=sorted(list(metrics.get_model_names().values()))
        )

        query = """
        select * from dbt-metrics-dw.analytics.{} limit 50
        """.format(selected_node_name)
    
    with st.spinner("Retrieving Data Table..."):
        df = metrics.get_query_results(query)
        df.columns = [c.lower() for c in df.columns]

        st.subheader(f"{selected_node_type[:-1]}: {selected_node_name}")
        st.dataframe(df)

elif tabs == 'dbt Metrics':
    st.title("dbt Metrics Example")

    expander = st.expander(label='Click to select filters')
    exp_col1, exp_col2, exp_col3 = expander.columns(3)

    col1, col2, col3 = st.columns(3)
    col1.metric("Net Charges", "$23M", "1.2%")
    col2.metric("Net Payments", "$6M", "-8%")
    col3.metric("Total Accounts Receivable", "$32M", "-0.2%")

    selected_metric_name = exp_col1.selectbox(
     label="Select a metric", options=sorted(list(metrics.get_metric_names().keys()))
    )
    
    selected_node_id = metrics.metrics_list[selected_metric_name]
    node = metrics.manifest["metrics"][selected_node_id]
    available_dimensions = node["dimensions"]
    available_time_grains = node["time_grains"]

    selected_dimension = exp_col2.selectbox(
        "Select a dimension", options=["No Dimension"] + available_dimensions
    )
    selected_time_grain = exp_col3.selectbox(
        "Select a time grain", options=available_time_grains
    )

    calculation_options = [
        'metrics.period_to_date(aggregate="average", period="year", alias="charges_this_year_average")',

        'metrics.rolling(aggregate="average", interval=3, alias="charges_avg_past_3_weeks")',
        'metrics.rolling(aggregate="average", interval=6, alias="charges_avg_past_6_weeks")',
        'metrics.rolling(aggregate="average", interval=12, alias="charges_avg_past_12_weeks")',
        'metrics.rolling(aggregate="average", interval=52, alias="charges_avg_past_52_weeks")',
        
        'metrics.rolling(aggregate="max", interval=3, alias="charges_max_past_3_weeks")',
        'metrics.rolling(aggregate="max", interval=6, alias="charges_max_past_6_weeks")',
        'metrics.rolling(aggregate="max", interval=12, alias="charges_max_past_12_weeks")',

        'metrics.rolling(aggregate="min", interval=3, alias="charges_min_past_3_weeks")',
        'metrics.rolling(aggregate="min", interval=6, alias="charges_min_past_6_weeks")',
        'metrics.rolling(aggregate="min", interval=12, alias="charges_min_past_12_weeks")',
        'metrics.period_to_date(aggregate="sum", period="year", alias="charges_this_year_total")'
    ]
    
    secondary_calcs_list = expander.multiselect(
        "Select secondary calculations", options=calculation_options
    )

    def get_min_max_dates(metric_name):
        
        return "2022-01-01", "2022-06-01"


    min_date, max_date = get_min_max_dates(selected_metric_name)

    if selected_dimension == "No Dimension":
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

    DEBUG = expander.checkbox("Debug Mode", value=False)
    if DEBUG:
        compiled = metrics._get_compiled_query(query)
        st.subheader("Compiled SQL")
        st.text(compiled)

    st.header('Selected Metric:')
    st.subheader(node["label"])
    st.markdown(node["description"])
    st.markdown(f'This metric is based on the model {node["model"]}. It is a {node["type"]} metric based on the column {node["sql"]}.')


    with st.spinner("Plotting results"):
        if selected_dimension == "No Dimension" == "No Dimension":
            maincharttitle=f"{selected_metric_name} over time"
            fig = px.line(df,x="period",y=selected_metric_name,title=maincharttitle)
        else:
            maincharttitle=f"{selected_metric_name} by {selected_dimension} over time"
            fig = px.line(df,x="period",y=selected_metric_name,color=selected_dimension,title=maincharttitle)

        st.plotly_chart(fig)

        st.subheader("Data Table")
        st.dataframe(df)

        behindthescenes_container = st.empty()
        behindthescenes_container.header('Behind the scenes...')

        querycol1,querycol2 = st.columns(2)
        compiled = metrics._get_compiled_query(query)

        querycol1.subheader("dbt Metric Config")
        querycol1.caption("This how the metric is configured in dbt.")
        querycol1.text(query)

        querycol2.subheader("dbt Compiled Query")
        querycol2.caption("This is the query that dbt is dynamically compiling based on your inputs")
        querycol2.text(compiled)



