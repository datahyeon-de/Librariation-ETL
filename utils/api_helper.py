# utils/api_helper.py
import os
import json
import gzip
import time
import logging
import requests
from queue import Queue
from datetime import datetime
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional
import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


def fetch_data(uri: str, params: Dict[str, Any], api_key: str, response_format: str = "json") -> Dict[str, Any]:
    query = urlencode({**params, "authKey": api_key, "format": response_format})
    full_url = f"{uri}?{query}"
    logger.info(f"Requesting URL: {full_url}")
    response = requests.get(full_url, timeout=10)
    response.raise_for_status()

    if response_format == "json":
        return response.json()
    elif response_format == "xml":
        return _parse_xml_to_dict(response.text)
    else:
        raise ValueError("Unsupported response format")


def _parse_xml_to_dict(xml_string: str) -> Dict[str, Any]:
    root = ET.fromstring(xml_string)
    return {child.tag: _xml_element_to_dict(child) for child in root}


def _xml_element_to_dict(elem: ET.Element) -> Any:
    if len(elem) == 0:
        return elem.text
    return {child.tag: _xml_element_to_dict(child) for child in elem}


def fetch_paginated_data(uri: str, params: Dict[str, Any], api_key: str, response_format: str = "json") -> List[Dict[str, Any]]:
    all_results = []
    page = 1
    while True:
        params["page"] = page
        try:
            data = fetch_data(uri, params, api_key, response_format)
            all_results.append(data)
            if "resultNum" in data and int(data["resultNum"]) < 1000:
                break
            page += 1
        except Exception as e:
            logger.error(f"Error during pagination at page {page}: {e}")
            break
    return all_results


def run_multithreaded_requests(
    uri: str,
    param_list: List[Dict[str, Any]],
    api_key: str,
    response_format: str = "json",
    use_pagination: bool = False,
    max_workers: int = 5,
    max_queue_size: int = 1000
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, int, List[Dict[str, Any]]]:
    results_queue = Queue(maxsize=max_queue_size)
    logs_queue = Queue(maxsize=max_queue_size)
    failed_params_queue = Queue()

    def task(params):
        start = time.time()
        try:
            if use_pagination:
                data = fetch_paginated_data(uri, params, api_key, response_format)
            else:
                data = [fetch_data(uri, params, api_key, response_format)]
            duration = round(time.time() - start, 3)
            results_queue.put(data)
            logs_queue.put({"params": params, "status": "success", "result_count": len(data), "duration_sec": duration})
        except Exception as e:
            duration = round(time.time() - start, 3)
            logs_queue.put({"params": params, "status": "fail", "error": str(e), "duration_sec": duration})
            failed_params_queue.put(params)
            logger.warning(f"Request failed: {params} ({e})")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(task, p) for p in param_list]
        for _ in as_completed(futures):
            pass

    results, logs, failed_params = [], [], []
    while not results_queue.empty():
        results.extend(results_queue.get())
    while not logs_queue.empty():
        logs.append(logs_queue.get())
    while not failed_params_queue.empty():
        failed_params.append(failed_params_queue.get())

    success_count = sum(1 for log in logs if log["status"] == "success")
    fail_count = sum(1 for log in logs if log["status"] == "fail")

    return results, logs, success_count, fail_count, failed_params


def run_serial_requests(
    uri: str,
    param_list: List[Dict[str, Any]],
    api_key: str,
    response_format: str = "json",
    use_pagination: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, int, List[Dict[str, Any]]]:
    results, logs, failed_params = [], [], []
    for params in param_list:
        start = time.time()
        try:
            if use_pagination:
                data = fetch_paginated_data(uri, params, api_key, response_format)
            else:
                data = [fetch_data(uri, params, api_key, response_format)]
            results.extend(data)
            logs.append({"params": params, "status": "success", "result_count": len(data), "duration_sec": round(time.time() - start, 3)})
        except Exception as e:
            logs.append({"params": params, "status": "fail", "error": str(e), "duration_sec": round(time.time() - start, 3)})
            failed_params.append(params)
            logger.warning(f"[Serial] Request failed: {params} ({e})")

    return results, logs, len(logs) - len(failed_params), len(failed_params), failed_params


def save_to_file(dag_id: str, data: List[Dict[str, Any]], execution_date: datetime, base_path: str = "data"):
    folder_path = os.path.join(base_path, dag_id)
    os.makedirs(folder_path, exist_ok=True)
    file_name = execution_date.strftime("%Y%m%d.json.gz")
    full_path = os.path.join(folder_path, file_name)

    with gzip.open(full_path, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info(f"Saved data to {full_path}")


def save_log_report(dag_id: str, execution_date: datetime, log_data: Dict[str, Any], base_path: str = "data"):
    folder_path = os.path.join(base_path, dag_id, "logs")
    os.makedirs(folder_path, exist_ok=True)
    ts = execution_date.strftime("%Y%m%dT%H%M%SZ")
    full_path = os.path.join(folder_path, f"{ts}.json")

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved log to {full_path}")
