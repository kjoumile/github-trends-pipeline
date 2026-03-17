import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime


class Github:
    __instance = None
    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            load_dotenv()
            cls.__instance = object.__new__(cls)
            cls.__instance.session = requests.Session()
            cls.__instance.__token = os.environ.get('GITHUB_TOKEN')
            cls.__instance._headers = {"Authorization": f"Bearer {os.environ.get('GITHUB_TOKEN')}"}
            cls.__instance.session.headers.update(cls.__instance._headers)
        return Github.__instance

    def get_trending(self, lang, sort, per_page):
        params = {
            'q': F'language:{lang}',
            'sort': sort,
            'per_page': per_page
        }
        with self.session.get("https://api.github.com/search/repositories", params=params) as response:
            return response.json()
    def save_trending(self, data, lang:str):
        with open(f"/opt/airflow/data/raw/response_{lang}_{datetime.now().strftime('%Y-%m-%d')}.json", 'w') as outfile:
            data['collected_at']=datetime.now().strftime('%Y-%m-%d')
            json.dump(data, outfile, indent=4)





