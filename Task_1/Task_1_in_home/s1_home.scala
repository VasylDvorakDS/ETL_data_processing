/*
chcp 65001 && spark-shell -i D:\ETL_data_processing\Task_1\Task_1_in_home\s1_home.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
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
    .option("usePlainNumberFormat", "true")
    .option("startColumn", 0)
    .option("endColumn", 99)
    .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
    .option("maxRowsInMemory", 20)
    .option("excerptSize", 10)
    .option("header", "true")
    .load("D:/ETL_data_processing/Task_1/Task_1_in_home/employee_jobs.xlsx")

  println("\n FIRST NORMAL FORM:\n")
  df1.show()

  println("\n SECOND NORMAL FORM:\n")

  var df_second_norm_form_table_1=df1.select("Employee_ID", "Name", "Home_city")
    .dropDuplicates()
  df_second_norm_form_table_1.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_home_1?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "second_norm_form_table_1")
    .mode("overwrite")
    .save()

  df_second_norm_form_table_1.show()

  var df_second_norm_form_table_2=df1.select("Home_city", "City_code")
    .dropDuplicates()
  df_second_norm_form_table_2.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_home_1?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "second_norm_form_table_2")
    .mode("overwrite")
    .save()

  df_second_norm_form_table_2.show()

var df_second_norm_form_table_3 = df1.select("Employee_ID", "Job_Code", "Job").dropDuplicates()
df_second_norm_form_table_3.write.format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark_task_home_1?user=root&password=driller")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "second_norm_form_table_3")
  .mode("overwrite")
  .save()

df_second_norm_form_table_3.show()

  println("\n THIRD NORMAL FORM:\n")

  var df_third_norm_form_table_1=df_second_norm_form_table_1
  df_third_norm_form_table_1.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_home_1?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "third_norm_form_table_1")
    .mode("overwrite")
    .save()
  
  df_third_norm_form_table_1.show()


  var df_third_norm_form_table_2=df_second_norm_form_table_2
  df_third_norm_form_table_2.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_home_1?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "third_norm_form_table_2")
    .mode("overwrite")
    .save()
  
  df_third_norm_form_table_2.show()


  var df_third_norm_form_table_3=df_second_norm_form_table_3.select("Employee_ID", "Job_Code")
    .dropDuplicates()
  df_third_norm_form_table_3.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_home_1?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "third_norm_form_table_3")
    .mode("overwrite")
    .save()
  
  df_third_norm_form_table_3.show()

  var df_third_norm_form_table_4=df_second_norm_form_table_3.select("Job_Code", "Job")
    .dropDuplicates()
  df_third_norm_form_table_4.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_home_1?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "third_norm_form_table_4")
    .mode("overwrite")
    .save()
  
  df_third_norm_form_table_4.show()

}

val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
