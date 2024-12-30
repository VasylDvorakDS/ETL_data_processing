import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
from pyspark.sql.functions import round as round2
import locale
locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')  # Устанавливаем локаль на русский

warnings.filterwarnings("ignore")
t0=time.time()
con = create_engine("mysql://root:driller@localhost:3306/spark_task_4")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
##spark=SparkSession.builder.appName("Hi").getOrCreate()
spark = SparkSession.builder \
    .appName("Hi") \
    .config("spark.jars", r"D:\spark\jars\mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

columns = ["id","category_id","rate","title","author"]
data = [("1", "1","5","java","author1"),
        ("2", "1","5","scala","author2"),
        ("3", "1","5","python","author3")]
if 1==11:
        df = spark.createDataFrame(data,columns)
        df.withColumn("id",col("id").cast("int"))\
        .withColumn("category_id",col("category_id").cast("int"))\
        .withColumn("rate",col("rate").cast("int"))\
        .withColumn("dt", current_timestamp())\
        .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark_task_4?user=root&password=driller")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4a")\
        .mode("overwrite").save()
        df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("D:/ETL_data_processing/Task_4/Task_4_in_office/s4.xlsx").where(col("title") == "news")\
        .withColumn("dt", current_timestamp())\
        .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark_task_4?user=root&password=driller")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4a")\
        .mode("append").save()
#Задача 2
with con.connect() as connection:
    sql.execute("""drop table if exists spark_task_4.`tasketl4b`""", connection)
    sql.execute("""CREATE TABLE if not exists spark_task_4.`tasketl4b` (
        `№` INT(10) NULL DEFAULT NULL,
        `Месяц` DATE NULL DEFAULT NULL,
        `Сумма платежа` FLOAT NULL DEFAULT NULL,
        `Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
        `Платеж по процентам` FLOAT NULL DEFAULT NULL,
        `Остаток долга` FLOAT NULL DEFAULT NULL,
        `проценты` FLOAT NULL DEFAULT NULL,
        `долг` FLOAT NULL DEFAULT NULL,
        `год` FLOAT NULL DEFAULT NULL,
        UNIQUE INDEX `№` (`№`) USING BTREE,
        INDEX `Месяц` (`Месяц`) USING BTREE
    )
    COLLATE='utf8mb4_0900_ai_ci'
    ENGINE=InnoDB""", connection)


from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("dataAdress", "'Лист2'!A1")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("D:/ETL_data_processing/Task_4/Task_4_in_office/s4_2.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))\
        .withColumn("год", round2(col("№")/12,1))
df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark_task_4?user=root&password=driller")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4b")\
        .mode("append").save()

from pyspark.sql.functions import to_date, col

from pyspark.sql.functions import to_date, col

# Указываем формат даты
df1 = df1.withColumn("Месяц", to_date(col("Месяц"), "dd.MM.yyyy"))
df2 = df1.toPandas()  # Конвертация в Pandas DataFrame
df2["Месяц"] = pd.to_datetime(df2["Месяц"], format="%d.%m.%Y", errors="coerce")


# Get current axis 
ax = plt.gca()
ax.ticklabel_format(style='plain')
# bar plot
df2.plot(kind='line', 
        x='№', 
        y='долг', 
        color='green', ax=ax)
df2.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='red', ax=ax)
# set the title 
plt.title('Выплаты')
plt.grid ( True )
ax.set(xlabel=None)
# show the plot 
plt.show() 
spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))
