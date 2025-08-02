from airflow import DAG
from airflow.utils.dates import days_ago
from plugins.operators.update_book_description import Data4LibraryUpdateBookDescriptionOperator

import pendulum

with DAG(
    dag_id='dag_data4library_update_book_description',
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
    tags=['update', 'data4library']
) as dag:

    task_update_book_description_test = Data4LibraryUpdateBookDescriptionOperator(
        task_id='task_update_book_description',
        mysql_conn_id='librariation_dev'  
    )

