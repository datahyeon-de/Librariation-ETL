from airflow import DAG
from plugins.operators.update_book_from_raw import BookSyncOperator
import pendulum

with DAG(
    dag_id='dag_data4library_update_book_from_raw',
    description='raw_loan_item_srch 테이블에서 book 테이블로 신규 데이터 동기화',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 8, 2, tz="Asia/Seoul"),
    catchup=False,
    tags=['DB', 'update', 'book'],
) as dag:

    task_update_book_from_raw = BookSyncOperator(
        task_id='task_update_book_sync_from_raw',
        mysql_conn_id='librariation_dev',
    )