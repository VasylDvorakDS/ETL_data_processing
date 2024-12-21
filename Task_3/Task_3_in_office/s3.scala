import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis
import scala.io.Source
val lines = Source.fromFile("/Users/Alex/Desktop/Geekbrains/Geekbrains_ETL/Seminars/s3.txt").getLines.toList
println("test " + lines(0))
var df1 = spark.read.format("com.crealytics.spark.excel")
    .option("sheetName", "Sheet1")
  .option("useHeader", "false")
  .option("treatEmptyValuesAsNulls", "false")
  .option("inferSchema", "true").option("addColorColumns", "true")
  .option("usePlainNumberFormat", "true")
  .option("startColumn", 0)
  .option("endColumn", 99)
  .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
  .option("maxRowsInMemory", 20)
  .option("excerptSize", 10)
  .option("header", "true")
  .format("excel")
  .load("/Users/Alex/Desktop/Geekbrains/Geekbrains_ETL/Seminars/s3.xlsx")
df1.show()
df1.write.format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=alex&password=1")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "tasketl3a")
  .mode("overwrite")
  .save()
println("task 1")
val q="""
SELECT 
    ID_тикета, 
    from_unixtime(StatusTime) AS StatusTime,
    (lead(StatusTime) OVER (PARTITION BY ID_тикета ORDER BY StatusTime) - StatusTime) / 3600 AS Длительность_статуса_на_текущей_группе,
    CASE 
        WHEN Статус IS NULL THEN @PREV1
        ELSE @PREV1 := Статус 
    END AS Статус,
    CASE 
        WHEN Группа IS NULL THEN @PREV2
        ELSE @PREV2 := Группа 
    END AS Группа,
    Назначения
FROM (
    SELECT 
        ID_тикета, 
        StatusTime, 
        Статус,
        IF(ROW_NUMBER() OVER (PARTITION BY ID_тикета ORDER BY StatusTime) = 1 AND Назначения IS NULL, '', Группа) AS Группа,
        Назначения
    FROM (
        SELECT 
            DISTINCT a.objectid AS ID_тикета, 
            a.restime AS StatusTime, 
            Статус, 
            Группа, 
            Назначения
        FROM (
            SELECT DISTINCT restime, objectid 
            FROM spark.tasketl3a 
            WHERE fieldname IN ('Status', 'GNAME2')
        ) a
    LEFT JOIN 
    (
        SELECT DISTINCT objectid, restime, fieldvalue AS Статус 
        FROM spark.tasketl3a a 
        WHERE fieldname IN ('Status')
    ) a1 
    ON a.OBJECTID = a1.objectid AND a.restime = a1.restime
    LEFT JOIN 
    (
    SELECT DISTINCT objectid, restime, fieldvalue AS Группа, 1 AS Назначения 
    FROM spark.tasketl3a a 
    WHERE fieldname IN ('GNAME2')
    ) a2 
    ON a.OBJECTID = a2.objectid AND a.restime = a2.restime
    )b1)b2

SELECT 
    Статус, 
    IFNULL(SUM(Длительность_статуса_на_текущей_группе) / COUNT(DISTINCT ID_тикета), 0) AS Время 
FROM tb 
GROUP BY 1
""""
spark.read.format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=alex&password=1")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("query", q)
  .load()
  .write
  .format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/spark?user=alex&password=1")
  .option("driver", "com.mysql.cj.jdbc.Driver")
  .option("dbtable", "tasketl3b")
  .mode("overwrite")
  .save()

val s = (System.currentTimeMillis() - t1) / 1000
val m = (s / 60) % 60
val h = (s / 60 / 60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)

