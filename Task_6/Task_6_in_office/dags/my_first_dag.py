from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def print_hello():
    return 'Hello world from first Airflow DAG!'

with DAG(
    'my_first_dag',
    description='Hello World DAG',
    schedule='0 12 * * *',  # Замена schedule_interval на schedule
    start_date=datetime(2017, 3, 20),
    catchup=False,
) as dag:
    hello_operator = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )

    start = EmptyOperator(task_id='start')

    start >> hello_operator  # Указание зависимостей
