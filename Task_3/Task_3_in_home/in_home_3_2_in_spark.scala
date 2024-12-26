/*
chcp 65001 && spark-shell -i D:\ETL_data_processing\Task_3\Task_3_in_home\in_home_3_2_in_spark.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

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

val t1 = System.currentTimeMillis()

// Загружаем данные из MySQL
val df = spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_3?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl3b") // Считываем все данные из tasketl3b
    .load()

// 1. Форматируем дату restime в нужном формате
val df_output = df.withColumn("formatted_time", date_format($"restime", "yyyy-MM-dd HH:mm"))

// 2. Преобразуем поле "Статус" в сокращения
.withColumn(
  "status_short",
  when($"Статус" === "Зарегистрирован", "ЗР")
    .when($"Статус" === "Назначен", "НЗ")
    .when($"Статус" === "Закрыт", "ЗТ")
    .when($"Статус" === "В работе", "ВР")
    .when($"Статус" === "Решен", "РШ")
    .otherwise("ИС")
)

// 3. Создаем колонку "con" с объединением formatted_time, status_short и Группа
.withColumn(
  "con",
  concat_ws("  ", $"formatted_time", $"status_short", $"Группа")
)

// 4. Группируем данные по "objectid" и объединяем значения "con" через "\n"
.groupBy("objectid")
  .agg(
    concat_ws("\n", collect_list($"con")).as("con")
  )

// 5. Записываем результат в таблицу tasketl3c
df_output.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_3?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl3d")
    .mode("overwrite")
    .save()

// Выводим время выполнения
val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)

