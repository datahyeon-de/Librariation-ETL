from airflow import DAG
from plugins.operators.load_loanItemSrch_to_raw_loanItemSrch import RawLoanItemSrchFileSaveToDBOperator
import pendulum

with DAG(
    dag_id='dag_data4library_load_loanItemSrch_to_raw_loanItemSrch',
    description='data4library - loanItemSrch 수집 데이터를 raw_loan_item_srch 테이블에 적재',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 8, 2, tz="Asia/Seoul"),
    catchup=False,
    tags=['loanItemSrch', 'load',],
) as dag:

    task_load_loanItemSrch_to_raw_loanItemSrch = RawLoanItemSrchFileSaveToDBOperator(
        task_id='task_load_loanItemSrch_to_raw_loanItemSrch',
        endpoint='loanItemSrch',
        mysql_conn_id='librariation_dev',  
        base_dir='/opt/airflow/files/data4library',
    )