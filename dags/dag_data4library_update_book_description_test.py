from airflow import DAG
from airflow.utils.dates import days_ago
from plugins.operators.data4library_update_book_description import Data4LibraryUpdateBookDescriptionOperator

import pendulum

with DAG(
    dag_id='dag_data4library_update_book_description_test',
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Seoul"),
    schedule_interval=None,  # 테스트용이므로 수동 실행
    catchup=False,
    tags=['test', 'update', 'data4library']
) as dag:

    task_update_book_description_test = Data4LibraryUpdateBookDescriptionOperator(
        task_id='task_update_book_description_test',
        mysql_conn_id='librariation_dev'  
    )

