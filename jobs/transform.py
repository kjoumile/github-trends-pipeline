import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
load_dotenv()
jar_path = os.path.abspath('../drivers/postgresql-42.7.3.jar')

class Spark:
    def __init__(self):
        self.jar_path = os.path.abspath('../drivers/postgresql-42.7.3.jar')
        self.__url = f"jdbc:postgresql://postgres:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        self.__user = os.getenv('POSTGRES_USER')
        self.__password = os.getenv('POSTGRES_PASSWORD')
        self.driver = 'org.postgresql.Driver'
        self.executor = 'spark.executor.extraClassPath'
        self.spark_session = None
        self.create_session()
        self.jdbc_options = {
            'driver': self.driver,
            'url': self.__url,
            'user': self.__user,
            'password': self.__password,
        }
        self._df = None


    def create_session(self):
        self.spark_session = SparkSession.builder.appName('Spark') \
        .config('spark.driver.extraClassPath', self.jar_path).config(self.executor, self.jar_path).getOrCreate()
        return  self.spark_session

    def read_from_postgres_spark(self, table='raw.repositories'):
        self._df = self.spark_session.read.format('jdbc') \
            .options(**self.jdbc_options) \
            .option('dbtable', table).load()
        return self._df

    def spark_run_transform(self):
        self._df = self.read_from_postgres_spark(table='raw.repositories')
        self._df = self._df.dropDuplicates()
        self._df = self._df.filter(
            col('name').isNotNull() & col('lang').isNotNull()
        )
        self.write_to_postgres_spark(table='staging.repositories')
        self.spark_session.stop()

    def write_to_postgres_spark(self, table='staging.repositories'):
        self._df.write.format('jdbc') \
            .options(**self.jdbc_options) \
            .option('dbtable', table) \
            .mode('overwrite').save()
