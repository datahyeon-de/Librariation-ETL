from airflow import DAG
from airflow.decorators import task
from plugins.operators.extract_api_save_to_flie import Data4LibraryAPISaveToFileOperator
from airflow.hooks.base import BaseHook

import pendulum

    
with DAG(
    dag_id="dag_data4library_extract_loanItemSrch_mt_monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    schedule_interval="0 23 1 * *",
    catchup=True,
    tags=['loanItemSrch', 'extract', 'monthly'],
) as dag:
    
    @task(task_id="get_age_startDt_endDt_list")
    def get_age_startDt_endDt_list(**context):
        ages = ["6", "8", "14", "20", "30", "40", "50", "60"]
        
        # context에서 data_interval_start와 data_interval_end 가져오기
        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
        
        # pendulum 객체를 문자열로 변환
        start_dt = data_interval_start.format('YYYY-MM-DD')
        end_dt = data_interval_end.subtract(days=1).format('YYYY-MM-DD')
        
        result = [
            {
                "age": age,
                "startDt": start_dt,
                "endDt": end_dt,
            } for age in ages
        ]
        
        return result
    
    task_extract_loanItemSrch_mt = Data4LibraryAPISaveToFileOperator(
        task_id="task_extract_loanItemSrch_mt",
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
    
    get_age_startDt_endDt_list() >> task_extract_loanItemSrch_mt