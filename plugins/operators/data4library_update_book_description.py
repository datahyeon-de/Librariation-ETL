from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from plugins.utils.log_helper import get_logger
from plugins.utils.db_utils import get_mysql_connection

import requests
import os
import pendulum

class Data4LibraryUpdateBookDescriptionOperator(BaseOperator):
    def __init__(self, mysql_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id

    def execute(self, context):
        run_time = pendulum.now().format('YYYYMMDDTHHmmss')
        
        log_name = "update_book_description"
        log_dir = "/opt/airflow/files/data4library/archive/logs"
        
        logger = get_logger(log_name, log_dir, run_time, stream=True)
        
        logger.info(f"DB 연결 시도: conn_id={self.mysql_conn_id}")
        
        try:
            conn = BaseHook.get_connection(self.mysql_conn_id)
            db = get_mysql_connection(conn.host, conn.login, conn.password, conn.schema)
            cursor = db.cursor()
            logger.info(f"DB 연결 성공: {conn.host}")
            logger.info(f"DB 연결 성공: {conn.login}")
        
        except Exception as e:
            logger.error(f"DB 연결 실패: {e}")
            raise e
        
        sql_path = '/opt/airflow/sql/select_book_description.sql'
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        logger.info("`book` 테이블에서 description이 NULL인 데이터 조회")
        cursor.execute(sql, (10,))
        books = cursor.fetchall()
        logger.info(f"수집 대상 도서 수: {len(books)}")
        
        api_key = Variable.get("LIBRARY_API_KEY")
        print(api_key)
        http_conn_id = "data4library.kr"
        connection = BaseHook.get_connection(http_conn_id)
        endpoint = "srchDtlList"
        
        base_url = f"http://{connection.host}:{connection.port}/api"
        full_url = f"{base_url}/{endpoint}"
        
        def _api_call(book):
            try:
                isbn13 = book['isbn13']
                book_id = book['book_id']
                
                params = {
                    "authKey": api_key,
                    "isbn13": isbn13,
                    "format": "json"
                }
                
                response = requests.get(full_url, params=params)
                print(response.url)
                response.raise_for_status()
                
                data = response.json()
                print(data)
                description = data["response"]["detail"][0]["book"]["description"]
                logger.info(f"도서 정보 수집 성공: {isbn13}")
                
                return (book_id, description)
            
            except Exception as e:
                logger.error(f"API 호출 실패: {isbn13} - err:{e}")
                
                return None

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_api_call, book) for book in books]
            results = [future.result() for future in as_completed(futures)]
        
        update_sql = """
            UPDATE book
            SET description = %s
            WHERE book_id = %s
        """
        

        for result in results:
            if not result:
                continue
            book_id, description = result
            print(book_id, description)
            cursor.execute(update_sql, (description[:1000], book_id))