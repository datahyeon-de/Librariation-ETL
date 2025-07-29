from airflow import DAG
from plugins.operators.data4library_file_save_to_db import Data4LibraryFileSaveToDBOperator
import pendulum

with DAG(
    dag_id='dag_data4library_load_loanItemSrch_file_to_db',
    description='data4library - loanItemSrch 수집 데이터 DB 적재',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Seoul"),
    catchup=False,
    tags=['loanItemSrch', 'load', 'manual'],
) as dag:

    task_load_loan_item_srch = Data4LibraryFileSaveToDBOperator(
        task_id='task_load_loan_item_srch',
        endpoint='loanItemSrch',
        mysql_conn_id='librariation_dev',  
        base_dir='/opt/airflow/files/data4library',
    ) 