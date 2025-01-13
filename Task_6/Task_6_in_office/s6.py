from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# Определение аргументов по умолчанию
default_args = {
    'owner': 'AGanshin',
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2022, month=6, day=1).in_timezone('Europe/Moscow'),
    'email': ['alex@alex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание объекта DAG
dag = DAG(
    'AGanshin_spark_etl',
    default_args=default_args,
    description='Запуск Spark ETL через Airflow',
    schedule_interval='0 6 * * *',
    catchup=False,
)

# Задача 1: Запуск Python-скрипта с помощью BashOperator
task1 = BashOperator(
    task_id='run_s6_script',
    bash_command='python3 /home/alex/s6.py',
    dag=dag,
)

# Задача 2: Настройка и запуск Spark shell через BashOperator
task2 = BashOperator(
    task_id='run_spark_shell',
    bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/alex/s6s1.scala',
    dag=dag,
)

# Установление порядка выполнения задач
task1 >> task2
