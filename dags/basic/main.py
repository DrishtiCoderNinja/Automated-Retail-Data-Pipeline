from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
import pymysql as pm
def conn_test():
    connection = BaseHook.get_connection("my_conn")
    password = connection.password
    username = connection.login
    host_url = connection.host
    database=connection.schema
    conn=pm.connect(host=host_url,user=username,password=password,database=database)
    print(conn)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'aaaaaaa_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    catchup=False
)

start_task = DummyOperator(task_id='start_task', dag=dag)

conn_test = PythonOperator(
    task_id='conn_test',
    python_callable=conn_test,
    dag=dag,
)

end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> conn_test >> end_task