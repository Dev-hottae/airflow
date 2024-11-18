
import pendulum

from airflow.models.dag import DAG
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

from airflow.decorators  import task


with DAG(
    dag_id="dags._base_branch_operator",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"],
) as dag:
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random

            print(context)
            item_lst = ['A','B','C']
            select_item = random.choice(item_lst)
            if select_item == 'A':
                return 'task_a'
            elif select_item in ['B','C']:
                return ['task_b','task_c']

    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')
    custom_branch_operator >> [task_a, task_b, task_c]