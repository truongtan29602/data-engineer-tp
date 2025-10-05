import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

with DAG(
    dag_id="second_dag_stub",
    start_date=START_DATE,
    schedule="0 0 * * *",   # daily at 00:00 UTC
    catchup=False,
    max_active_tasks=1,     # replaces old DAG-level 'concurrency'
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=["/opt/airflow/dags/"],
    tags=["stub"],
) as dag:

    get_spreadsheet = EmptyOperator(task_id="get_spreadsheet")
    transmute_to_csv = EmptyOperator(task_id="transmute_to_csv")
    time_filter = EmptyOperator(task_id="time_filter")

    emptiness_check = EmptyOperator(task_id="emptiness_check")
    split = EmptyOperator(task_id="split")

    create_intavolature_query = EmptyOperator(task_id="create_intavolature_query")
    create_composer_query = EmptyOperator(task_id="create_composer_query")

    insert_intavolature_query = EmptyOperator(task_id="insert_intavolature_query")
    insert_composer_query = EmptyOperator(task_id="insert_composer_query")

    join_tasks = EmptyOperator(task_id="coalesce_transformations")
    end = EmptyOperator(task_id="end")

    # Graph (structure preserved)
    get_spreadsheet >> transmute_to_csv >> time_filter >> emptiness_check
    emptiness_check >> [split, end]
    split >> [create_intavolature_query, create_composer_query]
    create_intavolature_query >> insert_intavolature_query
    create_composer_query >> insert_composer_query
    [insert_intavolature_query, insert_composer_query] >> join_tasks >> end
