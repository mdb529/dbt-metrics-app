import streamlit as st
import dbt
import json
import pathlib
import subprocess
import pandas
import os
from google.cloud import bigquery
from google.oauth2 import service_account


class MetricsUtil:
    def __init__(self):
        self.dbt_dir = pathlib.Path().cwd() / "dbt_metrics_app"
        self.client = self.get_db_clientection()
        self.manifest = self._read_manifest_file()
        self.metrics_list = self.get_metric_names()

    def _read_manifest_file(self):
        manifest_path = self.dbt_dir / "target" / "manifest.json"
        if not manifest_path.exists():
            self._install_project_dependencies()
            self._compile_project()
        text = manifest_path.read_text()
        return json.loads(text)

    def _compile_project(self):
        cmd = ["dbt", "compile", "--profiles-dir", "."]
        subprocess.run(cmd, cwd=self.dbt_dir.as_posix())

    def _install_project_dependencies(self):
        cmd = ["dbt", "deps", "--profiles-dir", "."]
        subprocess.run(cmd, cwd=self.dbt_dir.as_posix())

    def _build_project(self):
        cmd = ["dbt", "build", "--profiles-dir", "."]
        subprocess.run(cmd, cwd=self.dbt_dir.as_posix())

    def _run_project(self):
        cmd = ["dbt", "run", "--profiles-dir", "."]
        subprocess.run(cmd, cwd=self.dbt_dir.as_posix())

    def get_db_clientection(self):
        # clientect to BQ using the Provaiet key created above
        client = bigquery.Client(
            credentials= service_account.Credentials.from_service_account_file(st.secrets["serviceaccountkeyfile"]),
            project= st.secrets["project"]
            )
        return client

    def _get_compiled_query(self, raw_query):
        name = "dynamic_query.sql"
        output_model_path = self.dbt_dir / "models" / name
        compiled_query_path = (
            self.dbt_dir
            / "target"
            / "compiled"
            / "dbt_metrics_app"
            / "models"
            / name
        )
        output_model_path.write_text(raw_query)
        self._compile_project()
        query = compiled_query_path.read_text()
        return query

    def get_query_results(self, raw_query):
        compiled = self._get_compiled_query(raw_query)
        df = self.client.query(compiled).to_dataframe()
        return df

    def populate_template_query(
        self,
        metric_name,
        time_grain,
        dimensions_list=[],
        secondary_calcs_list=[],
        min_date="2022-01-01",
        max_date="2022-06-01",
    ):
        query = """
        select * from
        {{{{ 
            metrics.metric(
                metric_name='{metric_name}',
                grain='{time_grain}',
                dimensions={dimensions_list},
                secondary_calculations={secondary_calcs_list}
            )
        }}}}
        where period between date('{min_date}') and date('{max_date}')
        order by period
        """.format(
            metric_name=metric_name,
            time_grain=time_grain,
            dimensions_list=dimensions_list,
            secondary_calcs_list=secondary_calcs_list,
            min_date=min_date,
            max_date=max_date,
        )
        return query

    def get_metric_names(self):
        return {v["name"]: k for k, v in self.manifest["metrics"].items()}