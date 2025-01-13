from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pendulum
default_args = {
'owner': 'AGanshin',
'depends_on_past': False,
'start_date': pendulum.datetime(year=2022, month=6, day=1).in_timezone('Europe/Moscow'),
'email': ['alex@alex.ru'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 0,
'retry_delay': timedelta(minutes=5)
}
dag1 = DAG('AGanshin001',
default_args=default_args,
description="seminar_6",
catchup=False,
schedule_interval='0 6 * * *')
task1 = BashOperator(
task_id='pyspark',
bash_command='python3 /home/vasyl/s6.py',
dag=dag1)
task2 = BashOperator(
task_id='spark',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/vasyl/s6s1.scala',
dag=dag1)


dag2 = DAG('Vasyl',
default_args=default_args,
description="task_2",
catchup=False,
schedule_interval='0 7 * * *')
task21 = BashOperator(
task_id='practica',
bash_command='echo /home/vasyl/s6.py',
dag=dag2)


task22 = BashOperator(
task_id='practica_2',
bash_command='sh /home/vasyl/s6.sh ',
dag=dag2)





def func_1():
  return "task4"
task4 = PythonOperator(
task_id='practical_3',
python_callable = func_1, 
dag=dag2)
task21 >> [task22, task4]


