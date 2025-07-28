from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from typing import Dict

import requests
import os
import json
import time
import pendulum

from plugins.utils.log_helper import get_logger
from plugins.utils.api_helper import find_key_value

class Data4LibraryAPIManualOperator(BaseOperator):
    def __init__(self, endpoint: str, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = "data4library.kr"
        self.api_key = Variable.get("LIBRARY_API_KEY")
        self.endpoint = endpoint
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
        params = context.get("params", {})
        # 빈 값/None은 제외
        api_params = {k: v for k, v in params.items() if v not in (None, "")}
        api_params["authKey"] = self.api_key
        api_params["format"] = "json"

        # 파일명 생성용 파라미터 추출
        start_dt = api_params.get("startDt", "noSettingStart")
        end_dt = api_params.get("endDt", "noSettingEnd")

        # 디렉토리/파일명 생성 조건 분기
        if start_dt == "noSettingStart" and end_dt == "noSettingEnd":
            run_time = pendulum.parse(context['ts']).format('YYYYMMDDTHHmmss')
            base_dir = os.path.join(
                "/opt/airflow/files/data4library",
                self.endpoint,
                context['task'].task_id,
                run_time
            )
            base_filename = f"{context['task'].task_id}"
        else:
            dir_name = f"{start_dt}_{end_dt}"
            base_dir = os.path.join(
                "/opt/airflow/files/data4library",
                self.endpoint,
                context['task'].task_id,
                dir_name
            )
            base_filename = f"{context['task'].task_id}"
        os.makedirs(base_dir, exist_ok=True)
        data_file = os.path.join(base_dir, f"{base_filename}.json")
        log_file = os.path.join(base_dir, f"{base_filename}.log")
        logger = get_logger(context['task'].task_id, base_dir, f"{start_dt}_{end_dt}", log_file=log_file)

        logger.info(f"[MANUAL] Start {self.endpoint} API call")
        logger.info(f"[MANUAL] API params: {api_params}")

        url = f"{self.base_url}/{self.endpoint}"

        start_time = time.time()
        try:
            result = self._api_get(url, api_params)
            error_msg, error_flag = find_key_value(result, "error", max_depth=3)
            if error_flag:
                raise Exception(f"API 호출 오류: {error_msg}")
            logger.info(f"[MANUAL] API call success")
        except Exception as e:
            logger.error(f"[MANUAL] API 호출 에러: {e} (url: {url})")
            raise

        try:
            self._save_to_file(result, data_file)
            logger.info(f"[MANUAL] Save to file success: {data_file}")
        except Exception as e:
            logger.error(f"[MANUAL] 파일 저장 에러: {e} (경로: {data_file})")
            raise

        end_time = time.time()
        logger.info(f"[MANUAL] End {self.endpoint} API call")
        logger.info(f"[MANUAL] Time taken: {end_time - start_time} seconds") 