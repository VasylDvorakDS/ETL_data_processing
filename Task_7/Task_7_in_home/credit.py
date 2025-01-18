from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date, col
import pandas as pd
import os, sys
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Параметры подключения к MySQL
jdbc_url = "jdbc:mysql://localhost:33061/spark"
properties = {
    "user": "Airflow",
    "password": "1",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Установка конфигурации для Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName("Hi").getOrCreate()

def read_table(name_table, **kwargs):
    ti = kwargs['ti']
    df1 = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", name_table) \
        .options(**properties) \
        .load()

    # Указываем формат даты
    df1 = df1.withColumn("Месяц", to_date(col("Месяц"), "dd.MM.yyyy"))
    df2 = df1.toPandas()  # Конвертация в Pandas DataFrame
    df2["Месяц"] = pd.to_datetime(df2["Месяц"], format="%d.%m.%Y", errors="coerce")
    ti.xcom_push(key=name_table, value=df2)

def save_drawing(**kwargs):
    
    ti = kwargs['ti']
    df_without_repayment = ti.xcom_pull(key='Without_repayment')
    df_120000 = ti.xcom_pull(key='p_120000')
    df_150000 = ti.xcom_pull(key='p_150000')

    output_dir = '/home/vasyl'
    output_file = os.path.join(output_dir, 'credit_plot.png')

    ax = plt.gca()
    ax.ticklabel_format(style='plain')

    df_without_repayment.plot(kind='line', x='№', y='долг', color='green', ax=ax)
    df_without_repayment.plot(kind='line', x='№', y='проценты', color='green', linestyle='--', ax=ax)
    df_120000.plot(kind='line', x='№', y='долг', color='red', ax=ax)
    df_120000.plot(kind='line', x='№', y='проценты', color='red', linestyle='--', ax=ax)
    df_150000.plot(kind='line', x='№', y='долг', color='blue', ax=ax)
    df_150000.plot(kind='line', x='№', y='проценты', color='blue', linestyle='--', ax=ax)

    plt.title('Выплаты')
    plt.grid(True)
    ax.set(xlabel=None)
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    spark.stop()

with DAG(
    'credit_drawing',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['credit_drawing']
) as dag:
    
    read_table_Without_repayment_task = PythonOperator(
        task_id='read_table_Without_repayment',
        python_callable=read_table,
        op_kwargs={'name_table': 'Without_repayment'}
    )

    read_table_p_120000 = PythonOperator(
        task_id='read_table_p_120000',
        python_callable=read_table,
        op_kwargs={'name_table': 'p_120000'}
    )
    
    read_table_p_150000 = PythonOperator(
        task_id='read_table_p_150000',
        python_callable=read_table,
        op_kwargs={'name_table': 'p_150000'}
    )
    
    save_drawing_task = PythonOperator(
        task_id='save_drawing_task',
        python_callable=save_drawing   
    )

    [read_table_Without_repayment_task, read_table_p_120000, read_table_p_150000] >> save_drawing_task




