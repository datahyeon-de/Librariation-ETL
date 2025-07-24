from airflow import DAG
from plugins.operators.data4library_api_save_to_flie import Data4LibraryAPISaveToFileOperator
from datetime import datetime
import pendulum

with DAG(
    dag_id="dag_data4library_loanItemSrch_age8",
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    loan_item_srch_age8_task = Data4LibraryAPISaveToFileOperator(
        task_id="loan_item_srch_age8_task",
        endpoint="loanItemSrch",
        api_params={
            "age": "8",
        },
    )