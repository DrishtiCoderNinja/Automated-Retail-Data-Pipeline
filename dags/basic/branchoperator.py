from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
}

def decide_branch(**kwargs):
    if kwargs['execution_date'].day % 2 == 0:
        return 'even_branch'
    else:
        return 'odd_branch'

dag = DAG(
    'branch_operator',
    default_args=default_args,
    description='A simple example DAG with BranchPythonOperator',
    schedule_interval='@daily',
)

start = DummyOperator(task_id='start', dag=dag)

branch = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_branch,
     provide_context=True,
    dag=dag,
)

even_branch = DummyOperator(task_id='even_branch', dag=dag)
odd_branch = DummyOperator(task_id='odd_branch', dag=dag)

end = DummyOperator(task_id='end', dag=dag)

start >> branch >> [even_branch, odd_branch] >> end