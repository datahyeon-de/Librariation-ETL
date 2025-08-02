from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context
from plugins.utils.save_utils import extract_authors

import os
import json
import glob
import pymysql
import zipfile
import pendulum
import shutil


class RawLoanItemSrchFileSaveToDBOperator(BaseOperator):
    template_fields = ("endpoint",)

    def __init__(self, endpoint: str, mysql_conn_id: str, base_dir: str = "/opt/airflow/files/data4library", **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.mysql_conn_id = mysql_conn_id
        self.base_dir = base_dir

    def execute(self, context: Context):
        # JSON 파일 찾기
        json_files = glob.glob(os.path.join(self.base_dir, self.endpoint, "**", "*.json"), recursive=True)
        print(f"발견된 JSON 파일 수: {len(json_files)}")

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

        # SQL 쿼리 로드
        sql_path = '/opt/airflow/sql/insert_raw_loan_item_srch.sql'
        with open(sql_path, 'r', encoding='utf-8') as f:
            insert_sql = f.read()

        all_rows = []

        # JSON 파일 처리
        for file_path in json_files:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            req = data['response']['request']
            start_dt = req['startDt']
            end_dt = req['endDt']
            age = req['age']
            docs = data['response']['docs']

            for doc_wrap in docs:
                doc = doc_wrap['doc']

                row = {
                    'start_dt': start_dt,
                    'end_dt': end_dt,
                    'age': int(age),
                    'no': doc.get('no'),
                    'ranking': int(doc.get('ranking')) if doc.get('ranking') else None,
                    'bookname': doc.get('bookname'),
                    'authors': extract_authors(doc.get('authors')),
                    'publisher': doc.get('publisher'),
                    'publication_year': doc.get('publication_year'),
                    'isbn13': doc.get('isbn13'),
                    'addition_symbol': doc.get('addition_symbol') if doc.get('addition_symbol') else None,
                    'vol': doc.get('vol') if doc.get('vol') else None,
                    'class_no': doc.get('class_no'),
                    'class_nm': doc.get('class_nm'),
                    'bookImageURL': doc.get('bookImageURL'),
                    'bookDtlUrl': doc.get('bookDtlUrl'),
                    'loan_count': int(doc.get('loan_count', 0)) if doc.get('loan_count') else None
                }
                all_rows.append(row)

        # DB에 저장
        if all_rows:
            cursor.executemany(insert_sql, all_rows)
            db.commit()
            print(f"DB 저장 완료: {len(all_rows)}건")

        cursor.close()
        db.close()

        # 압축 및 아카이브 처리
        timestamp = pendulum.now().format('YYYYMMDDHHmmss')
        archive_dir = os.path.join('/opt/airflow/files', 'archive')
        os.makedirs(archive_dir, exist_ok=True)
        
        zip_filename = f"{self.endpoint}_{self.task_id}_{timestamp}.zip"
        zip_path = os.path.join(archive_dir, zip_filename)
        
        # 압축 파일 생성
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in json_files:
                arcname = os.path.relpath(file_path, self.base_dir)
                zipf.write(file_path, arcname)
        
        print(f"압축 파일 생성 완료: {zip_path}")
        
        # 원본 파일들 삭제
        try:
            # JSON 파일들이 있던 디렉토리 경로 수집
            dirs_to_check = set()
            for file_path in json_files:
                os.remove(file_path)
                dirs_to_check.add(os.path.dirname(file_path))
            
            print(f"원본 JSON 파일 삭제 완료: {len(json_files)}개")
            
            # 빈 디렉토리들 삭제 (하위 폴더부터)
            for dir_path in sorted(dirs_to_check, reverse=True):
                try:
                    if os.path.exists(dir_path) and not os.listdir(dir_path):
                        os.rmdir(dir_path)
                        print(f"빈 폴더 삭제: {dir_path}")
                except OSError:
                    pass  # 폴더가 비어있지 않거나 삭제할 수 없는 경우
                    
        except Exception as e:
            print(f"파일/폴더 삭제 실패: {e}")
        
        return len(all_rows)