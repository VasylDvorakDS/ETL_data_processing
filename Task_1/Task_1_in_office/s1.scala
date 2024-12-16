/*
chcp 65001 && spark-shell -i D:\ETL_data_processing\Task_1\Task_1_in_office\s1.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
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
import spark.sql

val t1 = System.currentTimeMillis()
if (1 == 1) {
  var df1 = spark.read.format("com.crealytics.spark.excel")
    .option("sheetName", "Sheet1")
    .option("useHeader", "false")
    .option("treatEmptyValuesAsNulls", "false")
    .option("inferSchema", "true")
    .option("addColorColumns", "true")
    .option("usePlainNumberFormat", "true")
    .option("startColumn", 0)
    .option("endColumn", 99)
    .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
    .option("maxRowsInMemory", 20)
    .option("excerptSize", 10)
    .option("header", "true")
    .load("D:/ETL_data_processing/Task_1/Task_1_in_office/Sem1.xlsx")
  
  df1.show()

  df1.filter(df1("Код предмета").isNotNull)
    .select("Код предмета", "Предмет", "Учитель")
    .write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_1?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl1a")
    .mode("overwrite")
    .save()

  val window1 = Window.partitionBy(lit(1))
    .orderBy("id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  df1.withColumn("id", monotonicallyIncreasingId)
    .withColumn("Код предмета", when(col("Код предмета").isNull, last("Код предмета", ignoreNulls = true).over(window1)).otherwise(col("Код предмета")))
    .orderBy("id")
    .drop("id", "Предмет", "Учитель")
    .write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_1?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl1b")
    .mode("overwrite")
    .save()

  println("task 1")
}

val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
