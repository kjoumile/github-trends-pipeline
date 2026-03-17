from airflow import DAG
from airflow.operators.python import PythonOperator

from extract.github_class import Github
from load.postgres_loader import PostgresLoader
from jobs.transform import Spark
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='github_trends',
    default_args=default_args,
    description='Github Trends',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['github'],
)

def extract_github_trends(**context):
    gh = Github()
    trending = gh.get_trending(lang='JAVA', sort='stars', per_page='30')
    gh.save_trending(trending, 'JAVA')

def load_to_github_trends(**context):
    pg = PostgresLoader()
    file_name=f"./data/raw/response_JAVA_{datetime.now().strftime('%Y-%m-%d')}.json"
    data = json.loads(open(file_name, 'r').read())
    pg.load(data)
    pg.disconnect()

def transform_github_trends(**context):
    spark = Spark()
    spark.spark_run_transform()


task_extract = PythonOperator(
    task_id='extract_github_trends',
    python_callable=extract_github_trends,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_github_trends',
    python_callable=load_to_github_trends,
    dag=dag,
)

task_transform= PythonOperator(
    task_id='transform_github_trends',
    python_callable=transform_github_trends,
    dag=dag,
)

task_extract >> task_load >> task_transform




