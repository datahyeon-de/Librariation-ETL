from airflow import DAG
from plugins.operators.data4library_api_save_to_flie import Data4LibraryAPISaveToFileOperator
from datetime import datetime
import pendulum

with DAG(
    dag_id="dag_data4library_extract_loanItemSrch_age8_monthly",
    start_date=pendulum.datetime(2025, 7, 30, tz="Asia/Seoul"),
    schedule_interval="0 23 1 * *",
    catchup=False,
    tags=['loanItemSrch', 'extract'],
) as dag:
    
    task_extract_loan_item_srch_age8 = Data4LibraryAPISaveToFileOperator(
        task_id="task_extract_loan_item_srch_age8",
        endpoint="loanItemSrch",
        api_params={
            "age": "8",
            "startDt": "{{ data_interval_start | ds }}",
            "endDt": "{{ (data_interval_end - macros.timedelta(days=1)) | ds }}",
        },
    )