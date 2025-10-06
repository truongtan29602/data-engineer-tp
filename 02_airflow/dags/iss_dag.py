import pendulum
import requests
import logging
import json
import numpy as np
import pandas as pd
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)
output_folder = "/opt/airflow/data"

def failure_alert(context):
    exc = context.get("exception")
    ti = context.get("ti")
    logger.error(
        f'FAILED task: dag={getattr(ti, "dag_id")}, task_id={getattr(ti, "task_id")}, run_id={context.get("run_id")}, try_number={getattr(ti, "try_number")}'
    )
    logger.exception(exc)

START_DATE = pendulum.datetime(2025, 10, 1, tz="UTC")

with DAG(
    dag_id = "iss_dag",
    start_date = START_DATE,
    schedule = None,
    catchup = False,
    max_active_tasks = 1,
    default_args = {
        "retries": 1,
        "retry_delay": timedelta(seconds = 10),
        "on_failure_callback": failure_alert
    },
    template_searchpath = ["/opt/airflow/data/"],
    tags = ["homework"]
) as dag:
    # ---------- HELPERS ----------
    def _fetch_current_location(api_url: str):
        try:
            response = requests.get(url = api_url, timeout = 30).json()
            location = response.get("iss_position")
            with open(f"{output_folder}/location.json", "w") as f:
                json.dump(location, f)
        except Exception as e:
            logger.exception("Error in _fetch_current_location")
            raise

    def haversine_np(lat1, lon1, lat2, lon2):
        R = 6371  # Earth radius in km
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat / 2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0)**2
        return R * 2 * np.arcsin(np.sqrt(a))

    def _find_nearest_country(output_folder: str, countries_csv: str):
        try:
            # Load ISS location
            with open(f"{output_folder}/location.json", "r") as read_file:
                data = json.load(read_file)
            lat, lon = float(data["latitude"]), float(data["longitude"])

            # Load countries CSV into DataFrame
            df = pd.read_csv(countries_csv)

            # Ensure numeric types
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

            df["distance"] = haversine_np(lat, lon, df["latitude"].values, df["longitude"].values)

            # Get nearest row
            nearest = df.loc[df["distance"].idxmin()]
            nearest_country = nearest["name"]

            # Write SQL
            with open(f"{output_folder}/nearest_country.sql", "w") as f:
                f.write(
                    "CREATE TABLE IF NOT EXISTS nearest_country (\n"
                    "   longitude REAL,\n"
                    "   latitude REAL,\n"
                    "   nearest_country TEXT\n"
                    ");\n"
                )
                f.write(
                    "INSERT INTO nearest_country VALUES ("
                    f"{lon}, {lat}, '{nearest_country}'"
                    ");\n"
                )

            logger.info(f"Nearest country: {nearest_country}, lat={lat}, lon={lon}")
        except Exception:
            logger.exception("Error in _find_nearest_country")
            raise


    # ----------  TASKS ----------
    fetch_current_location = PythonOperator(
        task_id = "fetch_current_location",
        python_callable = _fetch_current_location,
        op_kwargs = { 
            "api_url" : "http://api.open-notify.org/iss-now.json"
        }
    )

    find_nearest_country = PythonOperator(
        task_id = "find_nearest_country",
        python_callable = _find_nearest_country,
        op_kwargs = {
            "output_folder": output_folder,
            "countries_csv": "/opt/airflow/data/countries.csv"
        }
    )

    save_to_postgres = SQLExecuteQueryOperator(
        task_id = "save_to_postgres",
        conn_id = 'postgres_default',
        sql="nearest_country.sql",
        trigger_rule = 'none_failed',
        autocommit = True
    )

    end = BashOperator(
        task_id = "end",
        bash_command = "cat /opt/airflow/data/nearest_country.sql"
    )

    # ----------  GRAPH ----------
    fetch_current_location >> find_nearest_country >> save_to_postgres >> end