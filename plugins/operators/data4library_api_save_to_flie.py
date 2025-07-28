from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from typing import Dict

import requests
import os
import json
import time

from plugins.utils.log_helper import get_logger
from plugins.utils.api_helper import find_key_value

class Data4LibraryAPISaveToFileOperator(BaseOperator):
    template_fields = ("api_params",)  # api_params도 템플릿 필드로 지정
    
    def __init__(self, endpoint: str, api_params: Dict, **kwargs):
        super().__init__(**kwargs)
        
        self.http_conn_id = "data4library.kr"
        self.api_key = Variable.get("LIBRAR_API_KEY")
        self.endpoint = endpoint
        self.api_params = api_params
        
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
        task_id = context['task'].task_id
        # startDt, endDt를 api_params에서 추출 (템플릿 치환 후 값)
        start_dt = self.api_params.get("startDt", "unknownStart")
        end_dt = self.api_params.get("endDt", "unknownEnd")

        # 경로 생성: startDt_endDt를 포함
        base_dir = os.path.join(
            "/opt/airflow/files/data4library",
            self.endpoint,
            task_id,
            f"{start_dt}_{end_dt}"
        )
        os.makedirs(base_dir, exist_ok=True)

        # 파일명 생성
        base_filename = task_id
        # 데이터 파일 경로
        data_file = os.path.join(base_dir, f"{base_filename}.json")

        # 로그 파일 경로
        log_file = os.path.join(base_dir, f"{base_filename}.log")

        logger = get_logger(task_id, base_dir, f"{start_dt}_{end_dt}", log_file=log_file)

        logger.info(f"Start {self.endpoint} API call")
        logger.info(f"API params: {self.api_params}")

        self.api_params["authKey"] = self.api_key
        self.api_params["format"] = "json"

        url = f"{self.base_url}/{self.endpoint}"

        import time
        start_time = time.time()

        try:
            result = self._api_get(url, self.api_params)
            error_msg, error_flag = find_key_value(result, "error", max_depth=3)
            if error_flag:
                raise Exception(f"API 호출 오류: {error_msg}")
            logger.info(f"API call success")
        except Exception as e:
            logger.error(f"API 호출 에러: {e} (url: {url})")
            raise

        try:
            self._save_to_file(result, data_file)
            logger.info(f"Save to file success: {data_file}")
        except Exception as e:
            logger.error(f"파일 저장 에러: {e} (경로: {data_file})")
            raise

        end_time = time.time()
        logger.info(f"End {self.endpoint} API call")
        logger.info(f"Time taken: {end_time - start_time} seconds")