from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator



from datetime import datetime, date


def data_update():
  with open("a.csv","r+") as f:
    data = f.readlines()
    print(data)
    data[0]  = data[0][:-1] + f",Date\n"
    for i in range(1,len(data)):
      data[i] = data[i][:-1] + f",{date.today()}\n"
      print(i)
    
def data_save(filename,data):
  with open(filename,"w+") as f:
    f.writelines(data)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}
dag = DAG(
    'ccc',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    catchup=False,
)
    

start_task = DummyOperator(task_id='start_task', dag=dag)

task1 = PythonOperator(
    task_id='task1',
    python_callable=data_update(data_save("a.csv","w+")),
    dag=dag)

task2 = PythonOperator(
    task_id='task2',
    python_callable=data_save(),
    dag=dag)

end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> task1 >> task2>> end_task



