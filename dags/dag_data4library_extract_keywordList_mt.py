from airflow import DAG
from airflow.decorators import task
from plugins.operators.extract_api_save_to_flie import Data4LibraryAPISaveToFileOperator
from airflow.hooks.base import BaseHook
from plugins.utils.db_utils import get_mysql_connection

import pendulum

    
with DAG(
    dag_id="dag_data4library_extract_keywordList_mt",
    start_date=pendulum.datetime(2025, 8, 2, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
    tags=['keyword', 'extract',],
) as dag:
    
    @task(task_id="get_isbn13_list")
    def get_isbn13_list():
        conn = BaseHook.get_connection("librariation_dev")
        db = get_mysql_connection(conn.host, conn.login, conn.password, conn.schema)
        cursor = db.cursor()
        
        sql = """
            SELECT DISTINCT b.isbn13
            FROM book b
            WHERE b.updated_at > (
                SELECT COALESCE(MAX(bk.updated_at), '1900-01-01 00:00:00') 
                FROM book_keywords bk
            )
        """
        cursor.execute(sql)
        result = cursor.fetchall()
        return result
    
    task_extract_keywordList_mt = Data4LibraryAPISaveToFileOperator(
        task_id="task_extract_keywordList_mt",
        endpoint="keywordList",
        api_params={
            # "isbn13": "", 내부에서 처리
            "additionalYN": "Y"
        },
        sig_params = ["isbn13"],
        mt_option=True,
        mt_params={
            "max_workers": 10,
            "call_late": 0.25,
        },
        log_level="INFO",
    )
    
    get_isbn13_list() >> task_extract_keywordList_mt