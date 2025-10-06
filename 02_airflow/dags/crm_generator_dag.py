import pendulum
import requests
import logging
import pandas as pd
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)
output_folder = "/opt/airflow/data"
api_url = "https://randomuser.me/api/"

def failure_alert(context):
    exc = context.get("exception")
    ti = context.get("ti")
    logger.error(
        f'FAILED task: dag={getattr(ti, "dag_id")}, task_id={getattr(ti, "task_id")}, run_id={context.get("run_id")}, try_number={getattr(ti, "try_number")}'
    )
    logger.exception(exc)

START_DATE = pendulum.datetime(2025, 10, 6, tz="UTC")


with DAG(
    dag_id = "crm_generator_dag",
    start_date = START_DATE,
    schedule = None,
    catchup = False,
    max_active_tasks = 1,
    default_args = {
        "retries" : 1,
        "retry_delay" : timedelta(seconds = 10),
        "on_failure_callback" : failure_alert
    },
    template_searchpath = ["/opt/airflow/data/"],
    tags = ["homework"]
) as dag:
    # ---------- HELPERS ----------
    def _fetch_users(api: str, output_file: str):
        try:
            users = []
            for _ in range (0,5):
                response = requests.get(url = api, timeout = 30).json()
                users.append(response["results"][0])
            normalized_users = pd.json_normalize(users)
            normalized_users.to_csv(output_file, index = False)
        except Exception as e:
            logger.exception("Error in _fetch_users")
            raise

    # ----------  TASKS ----------
    fetch_users = PythonOperator(
        task_id = "fetch_user",
        python_callable = _fetch_users,
        op_kwargs = {
            "api" : api_url,
            "output_file" : f"{output_folder}/random_users.csv"
        }
    )

    end = EmptyOperator(
        task_id = "end_CRM_generator"
    )

    # ----------  GRAPHS ----------
    fetch_users >> end