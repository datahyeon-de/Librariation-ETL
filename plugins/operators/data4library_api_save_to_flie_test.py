from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.context import Context
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from plugins.utils.log_helper import get_logger
from plugins.utils.api_helper import find_key_value

import requests
import os
import json
import pendulum
import time


class Data4LibraryAPISaveToFileOperator(BaseOperator):
    template_fields = ("api_params",)  # api_params도 템플릿 필드로 지정
    
    def __init__(
        self, endpoint: str, api_params: Dict, sig_params: List[str], 
        mt_option: bool = False, mt_params: Optional[Dict] = None, 
        log_level: str = None, **kwargs):
        super().__init__(**kwargs)
        
        # 기본 설정
        self.http_conn_id = "data4library.kr"
        self.api_key = Variable.get("LIBRARY_API_KEY")
        self.endpoint = endpoint
        self.api_params = api_params # api 파라미터 설정
        self.mt_option = mt_option # 멀티 쓰레드 옵션
        self.mt_max_workers = 8 if mt_params is None else mt_params.get("max_workers", 8) # 멀티 쓰레드 최대 쓰레드 수
        self.mt_call_late = None if mt_params is None else mt_params.get("call_late", 0.25) # 멀티 쓰레드 쓰레드 대기 시간
        self.log_level = log_level # 로그 레벨 설정
        
        self.sig_params = sig_params
        self.sig_keys = "_".join(sig_params)  # 특정 파라미터 설정


    def _call_api(self, url: str, params: Dict) -> Dict:
        try:
            sig_name = "_".join(str(params[key]) for key in self.sig_params if key in params)
            
            if not sig_name:
                excluded_keys = {'authKey', 'format'}
                sig_name = "_".join(str(value) for key, value in params.items() if key not in excluded_keys)
            
            response = requests.get(url, params=params)
            
            response.raise_for_status()
            
            if self.mt_call_late:
                time.sleep(self.mt_call_late)
            
        except Exception as e:
            return (sig_name, {"error": str(e)})
        
        return (sig_name, response.json())


    # 파일 저장 함수
    def _save_to_file(self, result: Dict, save_path: str):
        
        # API 결과를 저장할 디렉토리 생성
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        # API 결과를 파일로 저장
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
            
            
    def _get_multi_thread_input_data(self, context: Context) -> List[Dict]: 
        upstream_task_ids = list(context['task'].upstream_task_ids)
        
        if not upstream_task_ids:
            return None
        
        upstream_task_id = upstream_task_ids[0]
        
        ti = context['ti']
        result = ti.xcom_pull(task_ids=upstream_task_id)
        
        return result


    # 실제 실행 함수
    def execute(self, context: Context):
        
        # API 요청 주소 설정
        connection = BaseHook.get_connection(self.http_conn_id)
        self.url = f"http://{connection.host}:{connection.port}/api/{self.endpoint}"
        
        # 테스크 아이디 추출 -> 파일 저장 및 로그에 사용
        task_id = context['task'].task_id
        
        # 테스크 실행 시간 추출 -> YYYYMMDDTHHmm
        run_time = pendulum.now().format('YYYYMMDDTHHmm')
        
        # API 결과 저장 디렉토리 생성 ->  /opt/airflow/files/data4library/{endpoint}/{task_id}
        result_save_dir = os.path.join("/opt/airflow/files/data4library", 
                                       self.endpoint, task_id, run_time)
        os.makedirs(result_save_dir, exist_ok=True)

        # API 결과 저장 파일명 생성
        result_save_file = f"result.json" 
        
        # API 결과 파일 전체 경로
        base_result_save_path = os.path.join(result_save_dir, result_save_file)

        # 로그 저장 디렉토리 생성
        log_save_dir = f"/opt/airflow/files/data4library/logs/{self.endpoint}"
        
        # 로그 저장 파일명 생성
        log_save_file = f"{self.sig_keys}_{task_id}_{run_time}.log" 
        
        # 성능 매트릭 저장 파일명 생성
        matrix_save_file = f"matrix_{self.sig_keys}_{task_id}_{run_time}.json"
        
        # 성능 매트릭 저장 경로
        matrix_save_path = os.path.join(log_save_dir, matrix_save_file)
        
        # 로그 초기화 및 출력
        logger = get_logger(
            name=task_id, log_dir=log_save_dir, log_file=log_save_file,
            level=self.log_level, 
            stream=True if self.mt_option else False
            )
        
        logger.info(f"[Start] Task: {task_id}")
        logger.info(f"================================================")
        
        # 로그 출력
        logger.info(f"[Start] {self.endpoint} API call")

        self.api_params["authKey"] = self.api_key
        self.api_params["format"] = "json"
        
        matrix = {
            "task_id": task_id,
            "run_time": run_time,
            "endpoint": self.endpoint,
            "sig_params": self.sig_params,
            "mt_option": self.mt_option,
            "mt_max_workers": self.mt_max_workers if self.mt_option else 0,
            "mt_call_late": self.mt_call_late if self.mt_option else 0,
            "total_input_data": 0,
            "total_api_call_success_count": 0,
            "total_api_call_fail_count": 0,
            "total_api_call_success_rate": 0,
            "total_file_save_success_count": 0,
            "total_file_save_fail_count": 0,
            "total_file_save_success_rate": 0,
            "total_time": "0 s",
            "avg_time": "0 s",
        }
        
        total_api_call_success_count = 0
        total_api_call_fail_count = 0
        total_file_save_success_count = 0
        total_file_save_fail_count = 0
        
        start_time = time.time()

        # 멀티 쓰레드 사용시
        if self.mt_option:
            input_data = self._get_multi_thread_input_data(context)
            
            # 이전 테스크에서 인풋 데이터 추출
            if input_data:
                
                for data in input_data:
                    data.update(self.api_params)

                logger.info(f"[SUCCESS] Get Multi Threading Input Data - Total: {len(input_data)}")
                
                logger.debug(f"Input Data: {input_data[:10]}")

                # 성능 매트릭 저장
                matrix["total_input_data"] = len(input_data)
                
            else:
                logger.error(f"[FAIL] Get Multi Threading Input Data - Total: 0")
                
                raise Exception("[FAIL] Get Multi Threading Input Data - Total: 0")
            
            # 멀티 쓰레드 수집 시작        
            try:
                logger.info(f"[Start] Multi Threading Call API")
                
                with ThreadPoolExecutor(max_workers=self.mt_max_workers) as executor:
                    futures = [executor.submit(self._call_api, self.url, data) for data in input_data]
                    results = [future.result() for future in futures]
                    
                for sig_name, result in results:
                    error_msg, error_flag = find_key_value(result, "error", max_depth=3)
                    
                    if error_flag:
                        logger.error(f"[FAIL] Call API: {sig_name} - Message: {error_msg}")
                        total_api_call_fail_count += 1
                        continue
                    
                    logger.info(f"[SUCCESS] Call API: {sig_name}")
                    
                    total_api_call_success_count += 1
                    
                    try:
                        result_save_path = base_result_save_path.replace('.json', f'_{sig_name}.json')
                        
                        self._save_to_file(result, result_save_path)
                        
                        logger.info(f"[SUCCESS] Save to File: {sig_name}")
                        
                        total_file_save_success_count += 1
                    
                    except Exception as e:
                        logger.error(f"[FAIL] Save to File: {sig_name} - Message: {e}")
                        
                        total_file_save_fail_count += 1
                        
                        continue
                
            except Exception as e:
                logger.error(f"[FAIL] Multi Threading Call API - Message: {e}")
                raise 
        
            finally:
                logger.info(f"[End] Multi Threading Call API")
        
        # Basic API Call 사용시
        else:
            # Basic API Call 수집 시작
            matrix["total_input_data"] = 1
            
            try:
                logger.info(f"[Start] Call API")
                
                sig_name, result = self._call_api(self.url, self.api_params)
                
                error_msg, error_flag = find_key_value(result, "error", max_depth=3)
                
                if error_flag:
                    logger.error(f"[FAIL] Call API: {sig_name} - Message: {error_msg}")
                    total_api_call_fail_count += 1
                    
                    raise Exception(error_msg)
                
                logger.info(f"[SUCCESS] Call API: {sig_name}")
                total_api_call_success_count += 1
                
                try:
                    result_save_path = base_result_save_path.replace('.json', f'_{sig_name}.json')
                    
                    self._save_to_file(result, result_save_path)
                    
                    logger.info(f"[SUCCESS] Save to File: {sig_name}")
                    total_file_save_success_count += 1
                    
                except Exception as e:
                    logger.error(f"[FAIL] Save to File: {sig_name} - Message: {e}")
                    total_file_save_fail_count += 1
                
            except Exception as e:
                logger.error(f"[FAIL] Call API - Message: {e}")
                raise
            
            finally:
                logger.info(f"[End] Call API")

        end_time = time.time()
        
        matrix["total_api_call_success_count"] = total_api_call_success_count
        matrix["total_api_call_fail_count"] = total_api_call_fail_count
        matrix["total_file_save_success_count"] = total_file_save_success_count
        matrix["total_file_save_fail_count"] = total_file_save_fail_count
        
        total_api_calls = total_api_call_success_count + total_api_call_fail_count
        total_file_saves = total_file_save_success_count + total_file_save_fail_count
        matrix["total_api_call_success_rate"] = f"{(total_api_call_success_count / total_api_calls * 100):.1f}%" if total_api_calls > 0 else "0%"
        matrix["total_file_save_success_rate"] = f"{(total_file_save_success_count / total_file_saves * 100):.1f}%" if total_file_saves > 0 else "0%"
        
        total_time_sec = end_time - start_time
        total_calls = total_api_call_success_count + total_api_call_fail_count
        avg_time_sec = total_time_sec / total_calls if total_calls > 0 else 0
        matrix["total_time"] = f"{total_time_sec:.2f} s"
        matrix["avg_time"] = f"{avg_time_sec:.2f} s"
        
        try:
            self._save_to_file(matrix, matrix_save_path)
            logger.info(f"[SUCCESS] Save Matrix: {matrix_save_file}")
            
        except Exception as e:
            logger.warning(f"[FAIL] Save Matrix: {e}")
        
        
        logger.info(f"[End] {self.endpoint} Call API & Save to File - Total Time: {end_time - start_time} seconds")

        logger.info(f"================================================")
        logger.info(f"[End] Task: {task_id}")