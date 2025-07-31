from airflow import DAG
from airflow.decorators import task
from plugins.operators.data4library_api_save_to_flie_test import Data4LibraryAPISaveToFileOperator
from airflow.hooks.base import BaseHook
from plugins.utils.db_utils import get_mysql_connection

import pendulum

    
with DAG(
    dag_id="dag_data4library_extract_keyword_mt_test",
    start_date=pendulum.datetime(2025, 7, 31, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
    tags=['keyword', 'extract', 'test'],
) as dag:
    
    @task(task_id="get_isbn13_list")
    def get_isbn13_list():
        conn = BaseHook.get_connection("librariation_dev")
        db = get_mysql_connection(conn.host, conn.login, conn.password, conn.schema)
        cursor = db.cursor()
        
        sql = """
            SELECT 
                isbn13
            FROM 
                book
            ;
        """
        cursor.execute(sql)
        result = cursor.fetchall()
        return result
    
    task_extract_keyword_mt_test = Data4LibraryAPISaveToFileOperator(
        task_id="task_extract_keyword_mt_test",
        endpoint="keywordList",
        api_params={
            # "isbn13": "", 내부에서 처리
            "additionalYN": "N"
        },
        sig_params = ["isbn13"],
        mt_option=True,
        mt_params={
            "max_workers": 10,
            "call_late": 0.25,
        },
        log_level="DEBUG",
    )
    
    get_isbn13_list() >> task_extract_keyword_mt_test