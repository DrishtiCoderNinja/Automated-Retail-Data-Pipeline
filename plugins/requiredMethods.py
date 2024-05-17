
import os
import shutil
import datetime
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook


def upload_folder():
    from datetime import date
    today = date.today()
    dd = today.strftime("%d-%m-%Y")
    s3_hook = S3Hook('s3_conn')
    folder_path="dags/retail_Project/processdata/"+str(dd)
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            local_path = os.path.join(root, file)
            print("local_path...",local_path)
            s3_key = local_path#folder_path + local_path.replace(folder_path, '').lstrip('/')
            print("s3_key....",s3_key)


            s3_hook.load_file(
                filename=local_path,
                key=s3_key,
                bucket_name="projectdata12",
                replace=True
            )

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
    """op_kwargs={
                'filename': 'dags/retail_Project/processdata/25-03-2023/location-wise-analysis/location-wise-analysis_25_03_2023.csv',#+str(dd),
                'key': f'StoreProcessData/{str(dd)}/result.csv',
                'bucket_name': 'projectdata12'
            }"""



def create_dir(temp_dir) -> None:
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    print("##### Dir created...")


def check_file():
    import os
    file_path = "dags/retail_Project/sourcedata/raw_store_transactions"
    check_file = os.path.isfile(file_path)
    if check_file == True:
        return "get_data_from_source"
    else:
        return "sending_email_without_file"


def data_cleaner():
    import pandas as pd
    import re
    from datetime import date

    df = pd.read_csv("dags/retail_Project/sourcedata/raw_store_transactions")

    def clean_store_location(st_loc):
        return re.sub(r'[^\w\s]', '', st_loc).strip()

    def clean_product_id(pd_id):
        matches = re.findall(r'\d+', pd_id)
        if matches:
            return matches[0]
        return pd_id

    def remove_dollar(amount):
        return float(amount.replace('$', ''))

    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: clean_store_location(x))
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: clean_product_id(x))
    df.insert(9, 'process_date', datetime.datetime.today().strftime('%Y-%m-%d'))

    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: remove_dollar(x))
    today = date.today()
    dd = today.strftime("%d-%m-%Y")
    dirname = "dags/retail_Project/processdata/" + str(dd)
    create_dir(dirname)
    file_name = "raw_store_transactions_" + dd.replace("-", "_")
    df.to_csv(dirname + "/" + file_name, index=False)


def load_data_into_table():
    connection = BaseHook.get_connection("mysql_8")
    password = connection.password
    username = connection.login
    host_url = connection.host
    port = connection.port
    import pymysql
    vertica_conn_info = {'host': host_url, 'port': port, 'user': username, 'password': password, 'database': "store"}
    connection_mysql = pymysql.connect(**vertica_conn_info)
    cur = connection_mysql.cursor()
    from datetime import date
    today = date.today()
    dd = today.strftime("%d-%m-%Y")
    dirname = "dags/retail_Project/processdata/" + str(dd)
    file_name = "/raw_store_transactions_" + dd.replace("-", "_")
    with open(dirname+file_name,'r') as f:
        h=f.readline()
        data=f.readlines()
        for i in data:
            xx=i.split("\n")[0].split(",")
            query=f"insert into store.daily_transactions values ('{xx[0]}','{xx[1]}','{xx[2]}','{xx[3]}','{xx[4]}','{xx[5]}','{xx[6]}','{xx[7]}','{xx[8]}','{xx[9]}')"
            cur.execute(query)
    connection_mysql.commit()

def doing_analysis(**kwargs):
    import matplotlib.pyplot as plt
    import pymysql
    import pandas as pd
    query_path=kwargs["query_path"]
    print("***********888store_query_path",query_path)
    file_loc=kwargs["file_loc"]
    file_name=kwargs['file_name']
    query=""
    connection = BaseHook.get_connection("mysql_8")
    password = connection.password
    username = connection.login
    host_url = connection.host
    port = connection.port
    vertica_conn_info = {'host': host_url, 'port': port, 'user': username, 'password': password, 'database': "store"}
    connection_mysql = pymysql.connect(**vertica_conn_info)
    cur = connection_mysql.cursor()
    with open(query_path,'r') as f:
        query=f.read()
        print("*********",query)
    cur.execute(query)
    table_rows = cur.fetchall()
    df = pd.DataFrame(table_rows)
    plot = df.plot.bar()
    fig = plot.get_figure()

    print(df.head(2))
    create_dir(file_loc)
    df.to_csv(file_loc+file_name,index=False)
    fig.savefig(file_loc + f"{file_name.split('.')[0]}.png")



import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

def send_email(**kwargs):
    subject=kwargs['subject']
    message=kwargs['message']
    from_email=kwargs['from_email']
    to_email=kwargs['to_email']
    password=kwargs['password']
    attachments = kwargs['attachments']
    """
    Sends an email message with attachments using the provided parameters.

    Args:
    - subject: The subject of the email.
    - message: The body of the email.
    - from_email: The email address that the email will be sent from.
    - to_email: The email address that the email will be sent to.
    - password: The password for the from_email account.
    - attachments: A list of file paths for the attachments (optional).

    Returns:
    - None
    """

    # Create the message object.
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    # Add the message body.
    body = MIMEText(message, 'html')
    msg.attach(body)

    # Add the attachments if any are provided.
    if attachments:
        for attachment in attachments:
            print("attachments....", attachments)
            with open(attachment, 'rb') as f:
                attachment_data = f.read()
            attachment_name = attachment.split('/')[-1]
            attachment_file = MIMEApplication(attachment_data, _subtype='pdf')
            attachment_file.add_header('Content-Disposition', 'attachment', filename=attachment_name)
            msg.attach(attachment_file)

    # Create the server object and login to the account.
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_email, password)

    # Send the email.
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()

