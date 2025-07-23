from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from typing import Dict
import requests
import os
import json
import time
from plugins.utils.log_helper import get_logger

class Data4LibraryAPISaveToFileOperator(BaseOperator):
    template_fields = ("save_path", "save_name")
    
    def __init__(
        self,
        endpoint: str, 
        api_params: Dict, 
        save_path: str, 
        save_name: str, 
        **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = "data4library.kr"
        self.api_key = Variable.get("LIBRARY_API_KEY")
        self.endpoint = endpoint
        self.api_params = api_params
        self.save_path = save_path
        self.save_name = save_name
        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f"http://{connection.host}:{connection.port}/api"

    def _api_get(self, url: str, params: Dict):
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _save_to_file(self, result: Dict, full_path: str):
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    def execute(self, context):
        dag_id = context['dag'].dag_id
        ds_nodash = context['ds_nodash']
        log_dir = f"/opt/airflow/files/data4library/{dag_id}/{ds_nodash}/log"
        logger = get_logger(dag_id, log_dir, ds_nodash)
        logger.info(f"Start {self.endpoint} API call")
        logger.info(f"API params: {self.api_params}")
        self.api_params["authKey"] = self.api_key
        self.api_params["format"] = "json"
        url = f"{self.base_url}/{self.endpoint}"
        start_time = time.time()
        try:
            result = self._api_get(url, self.api_params)
            logger.info(f"API call success")
        except Exception as e:
            logger.error(f"API 호출 에러: {e}")
            raise
        full_path = os.path.join(self.save_path, self.save_name)
        try:
            self._save_to_file(result, full_path)
            logger.info(f"Save to file success: {full_path}")
        except Exception as e:
            logger.error(f"파일 저장 에러: {e}")
            raise
        end_time = time.time()
        logger.info(f"End {self.endpoint} API call")
        logger.info(f"Time taken: {end_time - start_time} seconds")