from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context

import pymysql


class BookSyncOperator(BaseOperator):
    
    def __init__(self, mysql_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id

    def execute(self, context: Context):
        # MySQL 연결
        conn = BaseHook.get_connection(self.mysql_conn_id)
        db = pymysql.connect(
            host=conn.host,
            port=conn.port or 3306,
            user=conn.login,
            password=conn.password,
            database=conn.schema,
            charset='utf8mb4'
        )
        cursor = db.cursor()

        # SQL 파일 읽기
        sql_path = '/opt/airflow/sql/insert_book_from_raw.sql'
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()

        # 쿼리 실행
        cursor.execute(sql_query)
        affected_rows = cursor.rowcount
        db.commit()
        
        print(f"book 테이블 동기화 완료: {affected_rows}건 처리")

        cursor.close()
        db.close()
        
        return affected_rows