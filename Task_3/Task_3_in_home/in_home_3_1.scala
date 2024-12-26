/*
chcp 65001 && spark-shell -i D:\ETL_data_processing\Task_3\Task_3_in_home\in_home_3_1.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Закрываем возможную старую сессию
SparkSession.getActiveSession.foreach(_.stop())

// Создаём SparkSession
val spark = SparkSession.builder()
    .appName("ETL_Task")
    .config("spark.master", "local")
    .getOrCreate()

import spark.implicits._
import spark.sql

val t1 = System.currentTimeMillis()

val q = """
SELECT 
    objectid, 
    GROUP_CONCAT(
        CONCAT(DATE_FORMAT(restime, '%Y-%m-%d %H:%i'),'  ', 
            CASE 
                WHEN Статус = 'Зарегистрирован' THEN 'ЗР'
                WHEN Статус = 'Назначен' THEN 'НЗ'
                WHEN Статус = 'Закрыт' THEN 'ЗТ'
                WHEN Статус = 'В работе' THEN 'ВР'
                WHEN Статус = 'Решен' THEN 'РШ'
                ELSE 'ИС' 
            END, 
            '  ', Группа
        ) SEPARATOR '\n'
    ) AS con 
FROM tasketl3b
GROUP BY objectid
"""

spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_3?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("query", q)
    .load()
    .write
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_3?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl3c")
    .mode("overwrite")
    .save()


val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
