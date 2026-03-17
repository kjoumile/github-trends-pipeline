import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
load_dotenv()
jar_path = os.path.abspath('../drivers/postgresql-42.7.3.jar')
print(jar_path)
print(os.path.exists(jar_path))

spark = SparkSession.builder.appName('Spark').config('spark.driver.extraClassPath', jar_path) \
    .config('spark.executor.extraClassPath', jar_path) \
    .getOrCreate()
jdbc_options = {
    'url': f"jdbc:postgresql://localhost:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}",
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'driver': "org.postgresql.Driver",
}

def read_from_postgres_spark(table='raw.repositories'):
    df = spark.read.format('jdbc') \
        .options(**jdbc_options) \
        .option('dbtable',table).load()
    return df


def spark_run_transform():
    df = read_from_postgres_spark(table='staging.repositories')
    df = df.dropDuplicates()
    df = df.filter(
        col('name').isNotNull() & col('lang').isNotNull()
    )
    write_to_postgres_spark(df, table='staging.repositories')
    spark.stop()
def write_to_postgres_spark(df, table='staging.repositories'):
    df.write.format('jdbc') \
        .options(**jdbc_options) \
        .option('dbtable', table) \
        .mode('overwrite').save()