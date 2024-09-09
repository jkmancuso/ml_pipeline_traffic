from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from datetime import datetime

org = "personal"
bucket = "network"
start = "-30d"
drop = '"_start","_stop","_value","_field","_measurement"'
expected_field_num = 7

query = f"""from(bucket: \"{bucket}\") |> 
        range(start: {start}) |>
        drop (columns: [{drop}])"""


def pull_data(ti):
    val=str(ti.xcom_pull(task_ids="query_influx_task"))

    x = []

    #['', 'result', 'table', '_time', 'dest_ip', 'source_dns', 'source_ip']
    for i, line in enumerate(val.splitlines()):
        line_list = line.split(",")
        
        if i == 0 or len(line_list) != expected_field_num:
            continue
        
        hour = datetime.fromisoformat(line_list[3]).hour

        x.append(hour)

    print(x)
        

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
        data=query
    )
    python_task = PythonOperator(
        task_id="python_task",
        python_callable=pull_data
    )
    query_influx_task >> python_task
    