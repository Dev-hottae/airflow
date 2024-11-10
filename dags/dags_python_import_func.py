
import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import random
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"],
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp
    )

    task_get_sftp