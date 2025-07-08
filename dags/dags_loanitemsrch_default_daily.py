from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta

import pendulum

from config.config_setting import APISettingManager, DBSettingManager
from tasks.extract.task_extract_loanitemsrch import extract_default_loan_item_data
from tasks.transform.task_transform_loanitemsrch import transform_default_loan_item_data
from tasks.load.task_load_loanitemsrch import load_default_loan_item_data



api_manager = APISettingManager()
base_url = api_manager.get_api_base_url("loanItemSrch")
params = api_manager.get_api_params("data4library", "loanItemSrch")

db_manager = DBSettingManager()



with DAG(
    dag_id="dags_loanItemSrch_default_daily",
    start_date=pendulum.datetime(2025, 7, 7, tz="Asia/Seoul"),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["data4library", "loanItemSrch"],
) as dag:
    
    t1 = PythonOperator(
        task_id="extract_default_loan_item_data",
        python_callable=extract_default_loan_item_data,
        op_kwargs={
            "base_url": base_url,
            "params": params,
        },
    )
    
    t2 = PythonOperator(
        task_id="transform_default_loan_item_data",
        python_callable=transform_default_loan_item_data,
    )
    
    
    t3 = PythonOperator(
        task_id="load_default_loan_item_data",
        python_callable=load_default_loan_item_data,
        op_kwargs={
            "db_manager": db_manager,
            "table_name": "popular_loan_books_default",
        },
    )

    t1 >> t2 >> t3

