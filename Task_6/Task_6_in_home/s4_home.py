import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import os
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
from pyspark.sql.functions import round as round2
import locale
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import to_date, col
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1

locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')  # Устанавливаем локаль на русский
warnings.filterwarnings("ignore")
t0=time.time()
con=create_engine("mysql://Airflow:1@localhost/spark")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Hi").getOrCreate()

def create_table(name_table):
        name_table=str(name_table)
        with con.connect() as connection:
                sql.execute(f"""drop table if exists spark.`{name_table}`""", connection)
                sql.execute(f"""CREATE TABLE if not exists spark.`{name_table}` (
                        `№` INT(10) NULL DEFAULT NULL,
                        `Месяц` DATE NULL DEFAULT NULL,
                        `Сумма платежа` FLOAT NULL DEFAULT NULL,
                        `Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
                        `Платеж по процентам` FLOAT NULL DEFAULT NULL,
                        `Остаток долга` FLOAT NULL DEFAULT NULL,
                        `проценты` FLOAT NULL DEFAULT NULL,
                        `долг` FLOAT NULL DEFAULT NULL,
                        UNIQUE INDEX `№` (`№`) USING BTREE,
                        INDEX `Месяц` (`Месяц`) USING BTREE
                )
                COLLATE='utf8mb4_0900_ai_ci'
                ENGINE=InnoDB""", connection)

        w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        df1 = spark.read.format("com.crealytics.spark.excel")\
                .option("dataAddress", f"'{name_table}'!A1:F361")\
                .option("sheetName", name_table)\
                .option("useHeader", "false")\
                .option("treatEmptyValuesAsNulls", "false")\
                .option("inferSchema", "true").option("addColorColumns", "true")\
                .option("usePlainNumberFormat","true")\
                .option("startColumn", 0)\
                .option("endColumn", 10)\
                .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
                .option("maxRowsInMemory", 20)\
                .option("excerptSize", 10)\
                .option("header", "true")\
                .format("excel")\
                .load("/home/vasyl/Crerdit_calculator.xlsx").limit(1000)\
                .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
                .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
        df1.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=Airflow&password=1")\
                .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", name_table)\
                .mode("append").save()

        # Указываем формат даты
        df1 = df1.withColumn("Месяц", to_date(col("Месяц"), "dd.MM.yyyy"))
        df2 = df1.toPandas()  # Конвертация в Pandas DataFrame
        df2["Месяц"] = pd.to_datetime(df2["Месяц"], format="%d.%m.%Y", errors="coerce")
        return df2

df_without_repayment=create_table('Without_repayment')
df_120000=create_table('p_120000')
df_150000=create_table('p_150000')

# Directory to save diagram
output_dir = '/home/vasyl'
# File name diagram
output_file = os.path.join(output_dir, 'credit_plot.png')

# Get current axis 
ax = plt.gca()
ax.ticklabel_format(style='plain')
# bar plot
df_without_repayment.plot(kind='line', 
        x='№', 
        y='долг', 
        color='green', ax=ax)
df_without_repayment.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='green', linestyle='--', ax=ax)
df_120000.plot(kind='line', 
        x='№', 
        y='долг', 
        color='red', ax=ax)
df_120000.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='red', linestyle='--', ax=ax)
df_150000.plot(kind='line', 
        x='№', 
        y='долг', 
        color='blue', ax=ax)
df_150000.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='blue', linestyle='--', ax=ax)
# set the title 
plt.title('Выплаты')
plt.grid ( True )
ax.set(xlabel=None)

# Save credit_plot.png
plt.savefig(output_file, dpi=300, bbox_inches='tight')

# show the plot 
plt.show() 
spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))
