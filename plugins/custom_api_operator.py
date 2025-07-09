# plugins/operators/custom_api_operator.py
from airflow.models import BaseOperator
from airflow.utils.context import Context
from datetime import datetime
from typing import List, Dict, Any

from utils.api_helper import (
    run_multithreaded_requests,
    run_serial_requests,
    save_to_file,
    save_log_report,
)

class CustomAPIToFileOperator(BaseOperator):
    def __init__(
        self,
        uri: str,
        endpoint: str,
        params_list: List[Dict[str, Any]],
        api_key: str,
        use_pagination: bool = False,
        use_multithreading: bool = False,
        response_format: str = "json",
        base_save_path: str = "data",
        max_workers: int = 5,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.uri = uri.rstrip("/")
        self.endpoint = endpoint
        self.params_list = params_list
        self.api_key = api_key
        self.use_pagination = use_pagination
        self.use_multithreading = use_multithreading
        self.response_format = response_format
        self.base_save_path = base_save_path
        self.max_workers = max_workers

    def execute(self, context: Context):
        execution_date: datetime = context["execution_date"]
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        full_uri = f"{self.uri}/{self.endpoint}"

        start_time = datetime.utcnow()

        if self.use_multithreading:
            data, logs, succ, fail, failed_params = run_multithreaded_requests(
                uri=full_uri,
                param_list=self.params_list,
                api_key=self.api_key,
                response_format=self.response_format,
                use_pagination=self.use_pagination,
                max_workers=self.max_workers
            )
        else:
            data, logs, succ, fail, failed_params = run_serial_requests(
                uri=full_uri,
                param_list=self.params_list,
                api_key=self.api_key,
                response_format=self.response_format,
                use_pagination=self.use_pagination
            )

        save_to_file(dag_id, data, execution_date, base_path=self.base_save_path)

        end_time = datetime.utcnow()
        log_report = {
            "dag_id": dag_id,
            "task_id": task_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "total": len(self.params_list),
            "success": succ,
            "fail": fail,
            "logs": logs,
            "failed_params": failed_params
        }
        save_log_report(dag_id, execution_date, log_report, base_path=self.base_save_path)

        self.log.info(f"Total collected: {len(data)} records, Success: {succ}, Fail: {fail}")
