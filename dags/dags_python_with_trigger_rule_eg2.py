
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.decorators  import task


with DAG(
    dag_id="dags_base_branch_operator",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"],
) as dag:
    
    @task.branch(task_id='branching')
    def random_branch():
        import random
        item_lst = ['A','B','C']
        select_item = random.choice(item_lst)
        if select_item == 'A':
            return 'task_a'
        elif select_item == 'B':
            return 'task_b'
        elif select_item == 'C':
            return 'task_c'
    
    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo upstream1'
    )

    @task(task_id='task_b')
    def task_b():
        print('정상 처리')


    @task(task_id='task_c')
    def task_c():
        print('정상 처리')

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('정상 처리')

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()