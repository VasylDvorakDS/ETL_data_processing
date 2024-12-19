/*
chcp 65001 && spark-shell -i D:\ETL_data_processing\Task_2\Task_2_in_office\s2.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
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
    .csv("D:/ETL_data_processing/Task_2/Task_2_in_office/s2_data.csv")
  
  df1 = df1
    .withColumn("children", col("children").cast("int"))
    .withColumn("dob_years", col("dob_years").cast("int"))
    .withColumn("education_id", col("education_id").cast("int"))
    .withColumn("family_status_id", col("family_status_id").cast("int"))
    .withColumn("debt", col("debt").cast("int"))
    .withColumn("days_employed", col("days_employed").cast("float"))
    .withColumn("total_income", col("total_income").cast("float"))
    .dropDuplicates()
    .withColumn("purpose_category", 
      when(col("purpose").like("%авто%"), "операции с автомобилем")
      .when(col("purpose").like("%недвиж%") || col("purpose").like("%жиль%"), "операции с недвижимостью")
      .when(col("purpose").like("%образ%"), "получение образования")
      .when(col("purpose").like("%свадьб%"), "проведение свадьб")
    )
    .withColumn("total_income2",
      when(col("total_income").isNotNull, col("total_income"))
      .otherwise(avg("total_income").over(Window.partitionBy("income_type")))
    )
    .withColumn("education", lower(col("education")))
    .dropDuplicates()
  
  df1.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_2?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl2a")
    .mode("overwrite")
    .save()
  
  df1.show()

  val s = df1.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c))
  val df2 = df1.agg(s.head, s.tail: _*)
  val t = df2.columns.map(c => df2.select(lit(c).alias("col_name"), col(c).alias("null_count")))
  val df_agg_col = t.reduce((df1, df2) => df1.union(df2))
  df_agg_col.show()

  val ct = """
  CREATE TABLE IF NOT EXISTS `tasketl2b` (
    `Отдел` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
    `Начальник` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
    `Сотрудник` TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci'
  )
  COLLATE='utf8mb4_0900_ai_ci'
  ENGINE=InnoDB
  """

  val mysqlcon = "jdbc:mysql://localhost:3306/spark_task_2?user=root&password=driller"
  val driver = "com.mysql.cj.jdbc.Driver"

  import java.sql._

  def sqlexecute(sql: String): Unit = {
    var conn: Connection = null
    var stmt: Statement = null
    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(mysqlcon)
      stmt = conn.createStatement()
      stmt.executeUpdate(sql)
      println(s"$sql complete")
    } catch {
      case e: Exception => println(e)
    }
  }

  sqlexecute(ct)

  val a = Seq("a", "b", "c")
  val b = Seq("t1", "t2", "t3")

  sqlexecute("delete from tasketl2b")
  sqlexecute(s"insert into tasketl2b VALUES ('${a(0)}','${a(1)}','${a(2)}')")

  val df = spark.sparkContext.parallelize(List(
  ("Отдел1", b(0), "Сотрудник1"),
  ("Отдел2", b(1), "Сотрудник2"),
  ("Отдел3", b(2), "Сотрудник3")
)).toDF("Отдел", "Начальник", "Сотрудник")

  df.show()

  df.write
    .format("jdbc")
    .option("url", mysqlcon)
    .option("driver", driver)
    .option("dbtable", "tasketl2b")
    .mode("append")
    .save()
} finally {
  val s0 = (System.currentTimeMillis() - t1) / 1000
  val s = s0 % 60
  val m = (s0 / 60) % 60
  val h = (s0 / 60) / 60
  println(f"$h%02d:$m%02d:$s%02d")

  spark.stop()
}
