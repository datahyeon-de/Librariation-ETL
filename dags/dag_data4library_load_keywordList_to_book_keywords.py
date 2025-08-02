from airflow import DAG
from plugins.operators.load_keywordList_to_book_keywords import BookKeywordsFileSaveToDBOperator
import pendulum

with DAG(
    dag_id='dag_data4library_load_keywordList_to_book_keywords',
    description='data4library - keywordList 수집 데이터를 book_keywords 테이블에 적재',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 8, 2, tz="Asia/Seoul"),
    catchup=False,
    tags=['keywordList', 'load',],
) as dag:

    task_load_keywordList_to_book_keywords = BookKeywordsFileSaveToDBOperator(
        task_id='task_load_keywordList_to_book_keywords',
        endpoint='keywordList',
        mysql_conn_id='librariation_dev',  
        base_dir='/opt/airflow/files/data4library',
    )