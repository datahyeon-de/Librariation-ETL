from airflow import DAG
from airflow.decorators import task
from plugins.operators.extract_api_save_to_flie import Data4LibraryAPISaveToFileOperator
from airflow.hooks.base import BaseHook

import pendulum

    
with DAG(
    dag_id="dag_data4library_extract_loanItemSrch_mt_manual",
    start_date=pendulum.datetime(2025, 8, 2, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
    params={
        "age": "6,8,14,20,30,40,50,60",
        "yyyymm": "202501"
    },
    tags=['loanItemSrch', 'extract', 'manual'],
) as dag:
    
    @task(task_id="get_age_startDt_endDt_list_manual")
    def get_age_startDt_endDt_list_manual(**context):
        # params에서 age와 yyyymm 값 받기
        params = context['params']
        age_param = params.get('age', '6,8,14,20,30,40,50,60')
        yyyymm_param = params.get('yyyymm', '202501')
        
        # ","로 구분된 값들을 리스트로 변환
        ages = [age.strip() for age in age_param.split(',')]
        yyyymm_list = [yyyymm.strip() for yyyymm in yyyymm_param.split(',')]
        
        result = []
        
        # 각 yyyymm에 대해 첫째날과 마지막날 계산
        for yyyymm in yyyymm_list:
            year = int(yyyymm[:4])
            month = int(yyyymm[4:6])
            
            # 해당 월의 첫째날과 마지막날
            start_date = pendulum.datetime(year, month, 1, tz="Asia/Seoul")
            end_date = start_date.end_of('month')
            
            start_dt = start_date.format('YYYY-MM-DD')
            end_dt = end_date.format('YYYY-MM-DD')
            
            # 각 age와 조합하여 결과 생성
            for age in ages:
                result.append({
                    "age": age,
                    "startDt": start_dt,
                    "endDt": end_dt,
                })
        
        return result
    
    task_extract_loanItemSrch_mt_manual = Data4LibraryAPISaveToFileOperator(
        task_id="task_extract_loanItemSrch_mt_manual",
        endpoint="loanItemSrch",
        api_params={
            "pageSize" : 300
            },
        sig_params = ["age", "startDt", "endDt"],
        mt_option=True,
        mt_params={
            "max_workers": 10,
            "call_late": 0.25,
        },
        log_level="INFO",
    )
    
    get_age_startDt_endDt_list_manual() >> task_extract_loanItemSrch_mt_manual