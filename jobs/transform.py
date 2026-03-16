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
df = spark.read.format('jdbc').options(**jdbc_options).option('dbtable','raw.repositories').load()

df.show()
df.printSchema()

df = df.dropDuplicates()
df = df.filter(col('name').isNotNull() & col('lang').isNotNull())

df.write.format('jdbc').options(**jdbc_options).option('dbtable', 'staging.repositories').mode('overwrite').save()