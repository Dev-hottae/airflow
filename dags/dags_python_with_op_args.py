
import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import random
from common.common_func import regist

with DAG(
    dag_id="dags_python_with_op_args",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"],
) as dag:
    
    regist_t1 = PythonOperator(
        task_id='regist_t1',
        python_callable=regist,
        op_args=['htLim','man','kr','seoul']
    )

    regist_t1