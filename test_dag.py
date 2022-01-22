from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def print_hello():
    return 'Hello world from first Airflow DAG!'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@paypal.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('PPAD_hello_world', default_args=default_args, description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='PPAT_hello_task', python_callable=print_hello, dag=dag)

hello_operator
