from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from plugins.utils.utils import make_param_combinations, save_log, save_csv, run_api_multithreaded
import requests
import os
import time
from urllib.parse import urlencode

class Data4LibraryAPItoCSVOperator(BaseOperator):
    template_fields = ("api_params", "path", "file_name")

    def __init__(self, dataset_nm, path, file_name, api_params, use_pagination=False, use_multithreading=False, max_workers=4, **kwargs):
        super().__init__(**kwargs)
        self.dataset_nm = dataset_nm
        self.path = path
        self.file_name = file_name
        self.api_params = api_params
        self.use_pagination = use_pagination
        self.use_multithreading = use_multithreading
        self.max_workers = max_workers
        self.http_conn_id = "data4library.kr"

    def _api_call(self, api_params):
        connection = BaseHook.get_connection(self.http_conn_id)
        api_key = Variable.get("LIBRARY_API_KEY", default_var=connection.password)
        base_url = f"http://{connection.host}/api/{self.dataset_nm}"
        api_params = api_params.copy()
        api_params["authKey"] = api_key
        api_params.setdefault("format", "json")
        # 페이지네이션 처리
        if self.use_pagination:
            all_data, page = [], 1
            while True:
                api_params["pageNo"] = page
                url = f"{base_url}?{urlencode(api_params)}"
                resp = requests.get(base_url, params=api_params)
                resp.raise_for_status()
                data = resp.json()
                docs = data.get("response", {}).get("docs", [])
                all_data.extend([d["doc"] for d in docs if "doc" in d])
                if not docs or len(docs) < int(api_params.get("pageSize", 200)):
                    break
                page += 1
            return all_data, api_params.copy(), url
        else:
            url = f"{base_url}?{urlencode(api_params)}"
            resp = requests.get(base_url, params=api_params)
            resp.raise_for_status()
            data = resp.json()
            docs = data.get("response", {}).get("docs", [])
            return [d["doc"] for d in docs if "doc" in d], api_params.copy(), url

    def execute(self, context):
        param_list = make_param_combinations(self.api_params)
        dag_id = context["dag"].dag_id
        run_date = context["ds_nodash"]  # YYYYMMDD 형식
        base_dir = os.path.join(self.path, dag_id, run_date)
        data_dir = os.path.join(base_dir, "data")
        logs_dir = os.path.join(base_dir, "logs")
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(logs_dir, exist_ok=True)
        all_logs = []
        total_start = time.time()
        results = []  # (data, params, url)
        logs = []
        # 2. 멀티/싱글 처리
        if self.use_multithreading:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            start_time = time.time()
            def task(params):
                t0 = time.time()
                try:
                    data, params_used, url = self._api_call(params)
                    duration = round(time.time() - t0, 3)
                    logs.append({"params": params_used, "status": "success", "count": len(data), "duration": duration, "url": url})
                    return (data, params_used, url)
                except Exception as e:
                    duration = round(time.time() - t0, 3)
                    logs.append({"params": params, "status": "fail", "error": str(e), "duration": duration, "url": url if 'url' in locals() else None})
                    return ([], params, url if 'url' in locals() else None)
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(task, p) for p in param_list]
                for f in as_completed(futures):
                    results.append(f.result())
            total_time = round(time.time() - start_time, 3)
        else:
            for p in param_list:
                t0 = time.time()
                try:
                    data, params_used, url = self._api_call(p)
                    logs.append({"params": params_used, "status": "success", "count": len(data), "duration": round(time.time() - t0, 3), "url": url})
                    results.append((data, params_used, url))
                except Exception as e:
                    logs.append({"params": p, "status": "fail", "error": str(e), "duration": round(time.time() - t0, 3), "url": url if 'url' in locals() else None})
        # 3. 파라미터별 파일명/로그 저장
        for log in logs:
            key_str = "_".join(str(log["params"].get(k, "")) for k in sorted(log["params"]) if k not in ["authKey", "format"])
            csv_path = os.path.join(data_dir, f"{key_str}.csv")
            log_path = os.path.join(logs_dir, f"{key_str}_log.json")
            # results에서 해당 params에 해당하는 data만 추출
            param_data = []
            for data, params_used, url in results:
                if params_used == log["params"]:
                    param_data = data
                    break
            save_csv(param_data, csv_path)
            log["data_count"] = len(param_data)
            log["data_size"] = os.path.getsize(csv_path) if os.path.exists(csv_path) else 0
            save_log(log_path, log)
            all_logs.append(log)
        # 4. 전체 로그 저장
        total_log = {
            "total_time": round(time.time() - total_start, 3),
            "total_params": len(param_list),
            "total_success": sum(1 for l in logs if l["status"] == "success"),
            "total_fail": sum(1 for l in logs if l["status"] == "fail"),
            "all_logs": all_logs
        }
        save_log(os.path.join(logs_dir, "summary_log.json"), total_log)
        return total_log