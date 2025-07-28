from airflow import DAG
from plugins.operators.data4library_api_save_to_file_manual import Data4LibraryAPIManualOperator

import pendulum

with DAG(
    dag_id="manual_data4library_loanItemSrch",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
    tags=['loanItemSrch', 'extract', 'manual'],
    params={ 
        "age": "",
        "startDt": "",
        "endDt": "",
    }
    
) as dag:
    manual_loan_item_srch_task = Data4LibraryAPIManualOperator(
        task_id="manual_loan_item_srch_task",
        endpoint="loanItemSrch"
    )