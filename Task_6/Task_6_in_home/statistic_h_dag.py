from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pendulum
default_args = {
'owner': 'Vasyl Dvorak',
'depends_on_past': False,
'start_date': pendulum.datetime(year=2022, month=6, day=1).in_timezone('Europe/Moscow'),
'email': ['dvorak1981@yandex.ru'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 0,
'retry_delay': timedelta(minutes=5)
}
dag1 = DAG('Statistic_from_Vasyl',
default_args=default_args,
description="in_home_Task_6",
catchup=False,
schedule_interval='0 7 * * *')


task3 = BashOperator(
task_id='Add_legend_in_table',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/vasyl/in_home_3_1.scala',
dag=dag1)

task4 = BashOperator(
task_id='Credit_calculator',
bash_command='python3 /home/vasyl/s4_home.py',
dag=dag1)


task3 >> task4