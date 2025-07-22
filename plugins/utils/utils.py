import os
import json
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def make_param_combinations(param_dict):
    """여러 값이 리스트로 들어온 파라미터 dict를 모든 조합의 리스트로 변환"""
    from itertools import product
    keys = list(param_dict.keys())
    values = [v if isinstance(v, list) else [v] for v in param_dict.values()]
    return [dict(zip(keys, prod)) for prod in product(*values)]

def save_log(log_path, log_data):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)

def save_csv(data, csv_path):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

def run_api_multithreaded(api_func, param_list, max_workers=4):
    results, logs = [], []
    start_time = time.time()
    def task(params):
        t0 = time.time()
        try:
            data = api_func(params)
            duration = round(time.time() - t0, 3)
            logs.append({"params": params, "status": "success", "count": len(data), "duration": duration})
            return data
        except Exception as e:
            duration = round(time.time() - t0, 3)
            logs.append({"params": params, "status": "fail", "error": str(e), "duration": duration})
            return []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(task, p) for p in param_list]
        for f in as_completed(futures):
            results.extend(f.result())
    total_time = round(time.time() - start_time, 3)
    return results, logs, total_time