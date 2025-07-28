from airflow import DAG
from plugins.operators.data4library_api_save_to_flie import Data4LibraryAPISaveToFileOperator
from datetime import datetime
import pendulum

with DAG(
    dag_id="dag_data4library_loanItemSrch_age50",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    schedule_interval="0 23 1 * *",
    catchup=True,
    tags=['loanItemSrch', 'extract'],
) as dag:
    
    loan_item_srch_age50_task = Data4LibraryAPISaveToFileOperator(
        task_id="loan_item_srch_age50_task",
        endpoint="loanItemSrch",
        api_params={
            "age": "50",
            "startDt": "{{ data_interval_start | ds }}",
            "endDt": "{{ (data_interval_end - macros.timedelta(days=1)) | ds }}",
        },
    )