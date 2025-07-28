from airflow import DAG
from plugins.operators.data4library_file_save_to_db import Data4LibraryFileSaveToDBOperator
import pendulum

with DAG(
    dag_id='dag_data4library_file_save_to_db_test',
    description='loanItemSrch/loan_item_srch_age6_task/2024-01-01_2024-01-31 DB 적재 테스트',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Seoul"),
    catchup=False,
    tags=['loanItemSrch', 'load', 'manual'],
) as dag:

    load_to_db = Data4LibraryFileSaveToDBOperator(
        task_id='load_loan_item_srch_age6_to_db',
        endpoint='loanItemSrch/loan_item_srch_age6_task/2024-01-01_2024-01-31',
        mysql_conn_id='librariation_dev',  
        base_dir='/opt/airflow/files/data4library',
    ) 