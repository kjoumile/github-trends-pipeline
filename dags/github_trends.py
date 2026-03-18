from airflow import DAG
from airflow.operators.python import PythonOperator

from extract.github_class import Github
from load.postgres_loader import PostgresLoader
from jobs.transform import Spark
from datetime import datetime, timedelta
import json
from config import LANGUAGES, SORT_ORDER, PER_PAGE

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
    for lang in LANGUAGES:
        trending = gh.get_trending(lang=lang, sort=SORT_ORDER, per_page=PER_PAGE)
        gh.save_trending(trending, lang)

def load_to_github_trends(**context):
    pg = PostgresLoader()
    for lang in LANGUAGES:
        file_name=f"/opt/airflow/data/raw/response_{lang}_{datetime.now().strftime('%Y-%m-%d')}.json"
        data = json.loads(open(file_name, 'r').read())
        pg.load(data)
    pg.disconnect()

def transform_github_trends(**context):
    spark = Spark()
    spark.spark_run_transform()

def load_analytics_table(**context):
    pg = PostgresLoader()
    pg.execute(
        """INSERT INTO analytics.top_repos (repo_id, created_at, forks_count, stargazers_count, lang)
        SELECT id, created_at, forks_count, stargazers_count, lang
        FROM staging.repositories
        ORDER BY stargazers_count DESC
        ON CONFLICT (repo_id) DO UPDATE SET
            stargazers_count = EXCLUDED.stargazers_count,
            forks_count = EXCLUDED.forks_count,
            created_at = EXCLUDED.created_at"""
    )

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
task_load_for_analytics = PythonOperator(
    task_id='load_analytics_table',
    python_callable=load_analytics_table,
    dag=dag,
)
task_extract >> task_load >> task_transform >> task_load_for_analytics




