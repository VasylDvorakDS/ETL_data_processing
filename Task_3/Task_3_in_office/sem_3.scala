/*
chcp 65001 && spark-shell -i D:\ETL_data_processing\Task_3\Task_3_in_office\sem_3.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
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
    .load("D:/ETL_data_processing/Task_3/Task_3_in_office/s3.xlsx")

    df1.write.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_3?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl3a")
    .mode("overwrite")
    .save()

}

val q="""SELECT objectid,FROM_UNIXTIME (restime)restime,(LEAD( restime) OVER(PARTITION BY objectid ORDER BY restime)-
restime)/3600 Длительность,
case when Статус IS NULL then @Prev_1 ELSE @Prev_1:=Статус END Статус,  
case when Группа IS NULL then @Prev_2 ELSE @Prev_2:=Группа END Группа  
FROM (SELECT objectid, restime, Статус, if(ROW_NUMBER()OVER(PARTITION 
BY  objectid ORDER BY restime)=1 AND Назначение IS NULL, '', Группа)Группа, Назначение  
FROM (SELECT a.objectid, a.restime, Статус, Группа, Назначение, (SELECT @Prev_1:=''), (SELECT @Prev_2:='') 
FROM (SELECT DISTINCT objectid, restime FROM tasketl3a WHERE fieldname IN ('Status', 'GNAme2'))a

LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Статус FROM tasketl3a WHERE fieldname IN ('Status'))a1
ON a.objectid = a1.objectid AND a.restime=a1.restime

LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue Группа,1 Назначение FROM tasketl3a WHERE fieldname IN ('gname2'))a2
ON a.objectid = a2.objectid AND a.restime=a2.restime)b1)b2"""

spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_3?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("query", q)
    .load()
    .write
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark_task_3?user=root&password=driller")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "tasketl3b")
    .mode("overwrite")
    .save()



val s0 = (System.currentTimeMillis() - t1) / 1000
val s = s0 % 60
val m = (s0 / 60) % 60
val h = (s0 / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
