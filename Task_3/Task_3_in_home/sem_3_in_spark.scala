/*
chcp 65001 && spark-shell -i D:\ETL_data_processing\Task_3\Task_3_in_home\sem_3_in_spark.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
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

// Шаг 1: Загрузка данных из MySQL
val df = spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_3?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl3a")
    .load()

// Шаг 2: Фильтрация данных
val statusDF = df.filter($"fieldname" === "Status").select($"objectid", $"restime", $"fieldvalue".as("Статус"))
val groupDF = df.filter($"fieldname" === "gname2").select($"objectid", $"restime", $"fieldvalue".as("Группа"))

// Шаг 3: Объединение данных
val joinedDF = statusDF
  .join(groupDF, Seq("objectid", "restime"), "outer")
  .withColumn("Назначение", when($"Группа".isNotNull, lit(1)).otherwise(lit(0)))

// Шаг 4: Оконные функции
val windowSpec = Window.partitionBy($"objectid").orderBy($"restime")

val processedDF = joinedDF
  .withColumn("ROW_NUMBER", row_number().over(windowSpec))
  .withColumn("Длительность", 
    (lead($"restime", 1).over(windowSpec).cast("long") - $"restime".cast("long")) / 3600)
  .withColumn("Статус", last($"Статус", ignoreNulls = true).over(windowSpec))
  .withColumn("Группа", last($"Группа", ignoreNulls = true).over(windowSpec))

// Шаг 5: Удаление вспомогательных колонок и сохранение результата
val resultDF = processedDF
  .select($"objectid", from_unixtime($"restime").as("restime"), $"Длительность", $"Статус", $"Группа")

resultDF.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_3?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl3b_spark")
    .mode("overwrite")
    .save()

val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)

