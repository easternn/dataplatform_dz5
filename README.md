# Руководство по запуску сессии Apache Spark под управлением Prefect
## Требования.
- Запущены сервисы HDFS, YARN и HIVE с базой данных postgresql.

## Шаг 1: Скачивание csv файла и загрузка на hdfs 
```bash
wget https://github.com/MainakRepositor/Datasets/blob/master/GDP.csv # скачивание csv файла содержащего данные по ввп
hdfs dfs -mkdir -p /input # создание директории на HDFS
hdfs dfs -put ~/GDP.csv /input # Загрузка файла
hdfs dfs -ls /input # Проверка
```
## Шаг 2. Создание и подготовка виртуального окружения
```bash
python3 -m venv myenv # Создаём виртуальное окружение
source myenv/bin/activate # Активация
pip install prefect pyspark onetl
```
## Шаг 3. Создание Python скрипта
Создайте файл
```bash
nano prefect_flow.py
```

Скопируйте туда следующий код
```python
from onetl.connection import SparkHDFS
from onetl.file import FileDFReader
from onetl.db import DBWriter
from onetl.file.format import CSV
from onetl.connection import Hive

from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

@task
def create_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("spark-with-yarn") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.sql.hive.metastore.uris", "thrift://tmpl-dn-01:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark

@task
def extract_data(spark):
    hdfs = SparkHDFS(host="tmpl-nn", port=9000, spark=spark, cluster="test")
    hdfs.check()
    reader = FileDFReader(connection=hdfs, format=CSV(delimiter=",", header=True), source_path="/input")
    df = reader.run(["GDP.csv"])

    return df

@task
def transform_data(df):
    df = df.withColumn("gdp_percent", col("gdp_percent").cast("float"))
    avg_gdp_df = df.groupBy("year").agg(avg("gdp").alias("average_gdp"))
    df = df.join(avg_gdp_df, on="year", how="inner")
    df = df.repartition("year")

    return df

@task
def load_data(df, spark):
    hive = Hive(spark=spark, cluster="test")
    writer = DBWriter(connection=hive, table="testdb.results", options={"if_exists": "replace_entire_table", "partitionBy": "year"})
    writer.run(df)

@flow
def process_data():
    spark_sess = create_session()
    edata = extract_data(spark_sess)
    tdata = transform_data(edata)
    load_data(tdata, spark_sess)

if __name__ == "__main__":
    process_data()

```
## Шаг 4. Проверка
Выполните
```bash
    python3 prefect_flow.py
```

Запустите
```bash
hive
```

Внутри выполните следующие запросы
```sql
    USE testdb;
    SELECT * FROM results LIMIT 10;
```
