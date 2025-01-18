from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
import requests
from sqlalchemy import create_engine
from pandas.io import sql
from datetime import timezone


default_args = {
    'owner': 'Vasyl_Dvorak',
    'depends_on_past': False,
    'start_date': pendulum.datetime(year=2025, month=1, day=1).in_timezone('Europe/Moscow'),
    'email': ['dvorak1981@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

con = create_engine("mysql://Airflow:1@localhost/spark")


def create_table():
    name_table = 'weather_table'
    with con.connect() as connection:
        sql.execute(f"""
            CREATE TABLE IF NOT EXISTS spark.`{name_table}` (
                `№` INT(10) NOT NULL AUTO_INCREMENT,
                `City` TEXT NULL DEFAULT NULL,
                `Temperature` FLOAT NULL DEFAULT NULL,
                `Date_time_UTC` TIMESTAMP NULL DEFAULT NULL,
                `Time_zone` TEXT NULL DEFAULT NULL,
                PRIMARY KEY (`№`)
            )
            COLLATE='utf8mb4_0900_ai_ci'
            ENGINE=InnoDB
        """, connection)


def get_temperature(**kwargs):
    cities = ["London", "Moscow", "Berlin", "Paris", "New York"]
    api_key = "02a65db2e7851264720ad047d919773f"
    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            response_json = response.json()
            temperature = round(float(response_json['main']['temp']) - 273.15, 2)
            city_name = response_json['name']
            timezone_seconds = response_json['timezone']
            sign_time_zone = "+" if timezone_seconds >= 0 else "-"
            time_zone = sign_time_zone + f"{abs(timezone_seconds) // 3600:02}:{(abs(timezone_seconds) % 3600) // 60:02}"
            timestamp_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            with con.connect() as connection:
                sql.execute(f"""INSERT INTO spark.`weather_table` (City, Temperature, Date_time_UTC, Time_zone) 
                                VALUES ('{city_name}', {temperature}, '{timestamp_str}', '{time_zone}')""", connection)
        else:
            raise Exception(f'Failed to load information about {city}')


with DAG(
    'Weather_to_MySQL',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['Weather_to_MySQL'],
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    get_temperature_task= PythonOperator(
        task_id='get_temperature_task',
        python_callable=get_temperature,
    )

    create_table_task >> get_temperature_task 
