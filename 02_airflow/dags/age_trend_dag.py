import pendulum
import logging
import jsons
from datetime import timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

def failure_alert(context):
    exc = context.get("exception")
    ti = context.get("ti")
    logger.error(
        f'FAILED task: dag={getattr(ti, "dag_id")}, task_id={getattr(ti, "task_id")}, run_id={context.get("run_id")}, try_number={getattr(ti, "try_number")}'
    )
    logger.exception(exc)

output_folder = "/opt/airflow/data"
START_DATE = pendulum.datetime(2025, 10, 6 tz = "UTC")

