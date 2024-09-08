from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from datetime import datetime

org = "personal"
bucket = "network"
start = "-30d"

def pull_data(ti):
    val=ti.xcom_pull(task_ids="query_influx_task")
    print(val)

with DAG(
    "my-dag", start_date=datetime(2024,9,7),
    schedule='@daily',
    catchup=False
):
    query_influx_task = SimpleHttpOperator(
        task_id="query_influx_task",
        http_conn_id="influx_https",
        endpoint=f"/api/v2/query?org={org}",
        method="POST",
        data=f"from(bucket: \"{bucket}\")|> range(start: {start})",
    )
    python_task = PythonOperator(
        task_id="python_task",
        python_callable=pull_data
    )
    query_influx_task >> python_task
    