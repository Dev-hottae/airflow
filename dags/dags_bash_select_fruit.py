
import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"],
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    bash_t1 >> bash_t2