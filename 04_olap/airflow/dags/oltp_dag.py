import pendulum
import pandas as pd
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)  # Airflow captures this per task
output_folder = "/opt/airflow/data" # ensure this folder exists and is writable

# --- DAG config ---
START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")
OLTP_CONN_ID = "postgres_default"

with DAG(
    dag_id = "oltp_dag",
    start_date = START_DATE,
    schedule = "0 0 * * *",
    catchup = False,
    max_active_tasks = 1,
    default_args = {
        "retries": 0,
        "retry_delay": timedelta(minutes = 5),
    },    
    template_searchpath=["/opt/airflow/data/"],
    tags = ["oltp"]
) as dag:
    def transform_oltp_to_olap():
        hook = PostgresHook(postgres_conn_id = "postgres_default")
        conn = hook.get_conn()
        df = pd.read_sql("SELECT * FROM products;", conn)
        conn.close()
        print(df.head())
        logging.info(df.head())

    first_node = SQLExecuteQueryOperator(
        task_id = "init_oltp_schema",
        conn_id = "postgres_default",
        sql = "create_table_oltp.sql",
    )

    second_node = SQLExecuteQueryOperator(
        task_id = "insert_data_schema",
        conn_id = "postgres_default",
        sql = "data_oltp.sql",
    )
    third_node = PythonOperator(
        task_id = "transform",
        python_callable = transform_oltp_to_olap,
    )

    # -------- Graph --------
    first_node >> second_node >> third_node