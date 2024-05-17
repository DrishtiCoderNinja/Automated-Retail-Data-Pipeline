import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
#from operators.vsql_operator import VSQLOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
#from vertica_python import connect
# TODO: Jinja2 template here
from airflow.utils.trigger_rule import TriggerRule
from datetime import date
DAG_NAME = 'Retail_Data_Pipeline'
from requiredMethods import data_cleaner,check_file,load_data_into_table,doing_analysis,upload_to_s3,upload_folder,send_email
today = date.today()
dd = today.strftime("%d-%m-%Y")
from datetime import date
dag = DAG(
    dag_id=DAG_NAME,
    start_date=datetime.datetime.strptime( '2021-04-29', '%Y-%m-%d'),
    catchup=False
)

start = BashOperator(
    task_id='Start',
    do_xcom_push=True,
    bash_command="echo $(date '+%Y-%m-%d %H:%M:%S %p')",

    dag=dag
)
check_file = BranchPythonOperator(
    task_id='check_source_data',
    python_callable=check_file,
    dag=dag
)
get_data_from_source=PythonOperator(
    task_id='get_data_from_source',
    python_callable=data_cleaner,
   dag=dag
)
sql_path = 'createtable.sql'
create_mysql_table = MySqlOperator(task_id='create_mysql_table',
                                      mysql_conn_id="mysql_8", sql=sql_path,
                                   dag=dag)
load_data_into_table=PythonOperator(
    task_id='load_data_into_table',
    python_callable=load_data_into_table,
   dag=dag
)
query_path='dags/retail_Project/locationquery.sql'
ddd=dd.replace("-","_")
file_loc="dags/retail_Project/processdata/"+str(dd)+f"/location-wise-analysis/"
file_name=f"location-wise-analysis_{ddd}.csv"
loc_wise_analysis=PythonOperator(
    task_id='loc_wise_analysis',
    python_callable=doing_analysis,
    op_kwargs={'query_path': query_path,"file_loc":file_loc,"file_name":file_name},
   dag=dag
)
query_path='dags/retail_Project/storequery.sql'
ddd=dd.replace("-","_")
file_loc="dags/retail_Project/processdata/"+str(dd)+f"/store-wise-analysis/"
file_name=f"store-wise-analysis_{ddd}.csv"
store_wise_analysis=PythonOperator(
    task_id='store_wise_analysis',
    python_callable=doing_analysis,
    op_kwargs={'query_path': query_path,"file_loc":file_loc,"file_name":file_name},
   dag=dag
)

upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_folder,
        dag=dag)
subject="Retail Data Export-No Data"
message ="No data there....."
from_email="drishtiyog80@gmail.com"
to_email="drishtiyog80@gmail.com"
password="bdry xgne euth muzr"
attachments=None
sending_email_without_file=PythonOperator(
        task_id='sending_email_without_file',
        python_callable=send_email,
        op_kwargs={'subject': subject,"message":message,"from_email":from_email,'password':password,
                   'to_email':to_email,'attachments':None},
        dag=dag)
subject="Retail Data Export"
message ="Please verify this attachemnts"
from_email="drishtiyog80@gmail.com"
to_email="drishtiyog80@gmail.com"
password="bdry xgne euth muzr"
import os
filelist=[(os.path.abspath(os.path.join(dirpath, filename)), os.path.getsize(os.path.join(dirpath, filename)))
                for dirpath, dirnames, filenames in os.walk("dags/retail_Project/processdata/"+str(dd))
                for filename in filenames]

attachments=[ i[0] for i in filelist]

sending_email_with_file=PythonOperator(
        task_id='sending_email_with_file',
        python_callable=send_email,
        op_kwargs={'subject': subject,"message":message,"from_email":from_email,'password':password,
                   'to_email':to_email,'attachments':attachments},
        dag=dag)
end = BashOperator(
    task_id='End',
    do_xcom_push=True,
    bash_command="echo $(date '+%Y-%m-%d %H:%M:%S %p')",
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)


start >> check_file>>get_data_from_source>>create_mysql_table>>load_data_into_table>>[store_wise_analysis,
loc_wise_analysis]>>upload_to_s3>>sending_email_with_file>>end

check_file>>sending_email_without_file>>end














