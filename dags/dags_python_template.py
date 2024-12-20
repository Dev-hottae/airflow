
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task


with DAG(
    dag_id="dags_python_template",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"],
) as dag:
    
    def python_func1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id='python_t1',
        python_callable=python_func1,
        op_kwargs={
            'start_date': '{{ data_interval_start | ds }}',
            'end_date': '{{ data_interval_end | ds }}'
        }
    )

    @task(task_id='python_t2')
    def python_func2(**kwargs):
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts:' + kwargs['ts'])
        print('data_interval_start:' + str(kwargs['data_interval_start']))
        print('data_interval_end:' + str(kwargs['data_interval_end']))
        print('task_instance:' + str(kwargs['ti']))

    python_t1 >> python_func2()