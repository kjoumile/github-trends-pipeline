import psycopg2 as pg2
from dotenv import load_dotenv
import os
load_dotenv()
import json
class PostgresLoader:
    __instance = None
    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(PostgresLoader, cls).__new__(cls)
            cls.__instance._port = os.getenv('POSTGRES_PORT')
            cls.__instance._database = os.getenv('POSTGRES_DB')
            cls.__instance._user = os.getenv('POSTGRES_USER')
            cls.__instance._password = os.getenv('POSTGRES_PASSWORD')
            cls.__instance.connection = None
            cls.__instance.connect()
        return cls.__instance
    def connect(self):
        if self.connection is None:
            try:
                self.connection = pg2.connect(
                    user = self._user,
                    password = self._password,
                    port = self._port,
                    database = self._database,
                    host = 'localhost'
                )
                print('Connected to PostgreSQL')
            except Exception as e:
                print(e)
    def disconnect(self):
        if self.connection is not None and self.connection.closed:
            self.connection.close()

    def load(self, data):
        with self.connection.cursor() as cursor:
            try:
                for item in data['items']:
                    cursor.execute(f'''INSERT INTO raw.repositories (id, name, full_name,
                    lang, stargazers_count, forks_count, open_issues_count,
                    topics, created_at, updated_at, collected_at)
                    VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s) ON CONFLICT (id)
                    DO UPDATE SET
                        stargazers_count = EXCLUDED.stargazers_count,
                        forks_count = EXCLUDED.forks_count,
                        open_issues_count = EXCLUDED.open_issues_count,
                        topics = EXCLUDED.topics,
                        updated_at = EXCLUDED.updated_at,
                        collected_at = EXCLUDED.collected_at;''',
                                   (item['id'], item['name'], item['full_name'], item['language'],
                                    item['stargazers_count'], item['forks_count'], item['open_issues_count'],
                                    json.dumps(item['topics']), item['created_at'], item['updated_at'], data['collected_at']))
                self.connection.commit()
                print('Data loaded')
            except Exception as e:
                print(e)

def read_json(file_name):
    with open(file_name) as json_file:
        data = json.load(json_file)
    return data

