/*
chcp 65001 && spark-shell -i D:\ETL_data_processing\Task_2\Task_2_in_home\fifa.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Проверяем и закрываем старую сессию
SparkSession.getActiveSession.foreach(_.stop())

// Создаем новую SparkSession
val spark = SparkSession.builder()
  .appName("ETL_Task")
  .config("spark.master", "local")
  .getOrCreate()

import spark.implicits._

val t1 = System.currentTimeMillis()

try {
  var df1 = spark.read.option("delimiter", ",")
    .option("header", "true")
    .csv("D:/ETL_data_processing/Task_2/Task_2_in_home/fifa_s2.csv")

val rowCount = df1.count()
println(s"Количество строк в df1: $rowCount")

println("Подсчет null значений для каждого столбца")
val nullCounts = df1.columns.map { colName =>
  count(when(col(colName).isNull, 1)).alias(colName)
}

val nullSummary = df1.agg(nullCounts.head, nullCounts.tail: _*)

nullSummary.show()

df1=df1.na.drop(Seq("Club")).drop("Value", "Release Clause").na.fill("0")

    // Преобразование всех строковых столбцов в нижний регистр
var df_formated = df1.select(
  df1.columns.map(c =>
    when(col(c).isNotNull && col(c).cast("string").isNotNull, lower(col(c)))
      .otherwise(col(c))
      .alias(c)
  ): _*
).dropDuplicates()
  
  df1 = df_formated
    .withColumn("Age", col("Age").cast("int"))
    .withColumn("Age_category", 
      when(col("Age")<= 20, "до 20 лет")
      .when(col("Age") > 20 && col("Age") <=30, "от 20 до 30 лет")
      .when(col("Age") > 30 && col("Age") <=36, "от 30 до 36 лет")
      .when(col("Age") > 36, "больше 36 лет")
    )
  
  df1.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_2_home?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "fifa")
    .mode("overwrite")
    .save()

 println("Окончательный DataFrame") 
  df1.show()

val df_age = df1
  .groupBy("Age_category")
  .count()
  .withColumnRenamed("count", "Quantity")
  .orderBy($"Quantity".desc)

df_age.write.format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark_task_2_home?user=root&password=driller")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "age_category")
  .mode("overwrite")
  .save()

println("Таблица распределения по возростам")
df_age.show()

} finally {
  val s0 = (System.currentTimeMillis() - t1) / 1000
  val s = s0 % 60
  val m = (s0 / 60) % 60
  val h = (s0 / 60) / 60
  println(f"$h%02d:$m%02d:$s%02d")

  spark.stop()
}
