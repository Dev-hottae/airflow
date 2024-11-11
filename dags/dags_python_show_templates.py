
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task


with DAG(
    dag_id="dags_python_show_templates",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 11, 1, tz="Asia/Seoul"),
    catchup=True,
    tags=["example", "example2"],
) as dag:
    
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()