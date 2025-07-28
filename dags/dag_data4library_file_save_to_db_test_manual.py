from airflow import DAG
from plugins.operators.data4library_file_save_to_db import Data4LibraryFileSaveToDBOperator
import pendulum

with DAG(
    dag_id='dag_data4library_file_save_to_db_test_manual',
    description='loanItemSrch DB 적재 테스트 매뉴얼',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Seoul"),
    catchup=False,
    tags=['loanItemSrch', 'load', 'manual'],
    params={"test_path": "loanItemSrch"}  # ✅ 기본값, 트리거 시 override 가능
) as dag:

    load_loan_item_srch_file_to_db_test = Data4LibraryFileSaveToDBOperator(
        task_id='load_loan_item_srch_file_to_db_test',
        endpoint="{{ params.test_path }}",  # ✅ 템플릿 문법 사용
        mysql_conn_id='librariation_dev',
        base_dir='/opt/airflow/files/data4library',
    )