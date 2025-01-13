from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# Определение аргументов по умолчанию для DAG
default_args = {
    'owner': 'AGanshin',
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2022, month=6, day=1).in_timezone('Europe/Moscow'),
    'email': ['alex@alex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
dag1 = DAG(
    'AGanshin001',
    default_args=default_args,
    description="seminar_6",
    catchup=False,
    schedule_interval='0 6 * * *',
)

# Определение задач

# Задача 1: Запуск скрипта pyspark
task1 = BashOperator(
    task_id='pyspark',
    bash_command='python3 /home/alex/s6.py',
    dag=dag1,
)

# Задача 2: Запуск Spark shell с использованием скрипта Scala
task2 = BashOperator(
    task_id='spark',
    bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/alex/s6s1.scala',
    dag=dag1,
)

# Задание зависимости между задачами (если требуется)
# task1 >> task2
