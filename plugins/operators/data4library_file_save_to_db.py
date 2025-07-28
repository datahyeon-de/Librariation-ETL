from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context
from plugins.utils.log_helper import get_logger
from plugins.utils.save_utils import extract_authors
from plugins.utils.data4library_schema import Data4LibraryLoanItemValidator

import os
import json
import glob
import zipfile
import pendulum
import pymysql

class Data4LibraryFileSaveToDBOperator(BaseOperator):
    def __init__(self, endpoint: str, mysql_conn_id: str, base_dir: str = "/opt/airflow/files/data4library", **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.mysql_conn_id = mysql_conn_id
        self.base_dir = base_dir

    def execute(self, context: Context):
        # 로그 설정
        run_time = pendulum.now().format('YYYYMMDDTHHmmss')
        log_dir = os.path.join(self.base_dir, self.endpoint, f"save_to_db_{run_time}")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"save_to_db_{run_time}.log")
        logger = get_logger(self.task_id, log_dir, run_time, log_file=log_file)

        logger.info(f"파일 탐색 시작: {self.base_dir}/{self.endpoint}")
        json_files = glob.glob(os.path.join(self.base_dir, self.endpoint, "**", "*.json"), recursive=True)
        logger.info(f"발견된 JSON 파일 수: {len(json_files)}")

        # DB 연결
        logger.info(f"MySQL 연결 시도: conn_id={self.mysql_conn_id}")
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
        logger.info("MySQL 연결 성공")

        # SQL 파일에서 쿼리 읽기
        sql_path = '/opt/airflow/sql/insert_loan_item_srch.sql'
        with open(sql_path, 'r', encoding='utf-8') as f:
            insert_sql = f.read()

        all_rows = []
        fail_rows = []
        fail_msgs = []
        total_rows = 0
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                req = data['response']['request']
                startDt = req['startDt']
                endDt = req['endDt']
                age = req['age']
                docs = data['response']['docs']
                for doc_wrap in docs:
                    doc = doc_wrap['doc']
                    # id 생성: YYYYMM_isbn13_addition_symbol
                    yyyymm = startDt[:4] + startDt[5:7]
                    id_val = f"{yyyymm}_{doc.get('isbn13', '')}_{doc.get('addition_symbol', '')}"
                    row = {
                        'id': id_val,
                        'startDt': startDt,
                        'endDt': endDt,
                        'age': age,
                        'ranking': doc.get('ranking', 0) if doc.get('ranking') else None,
                        'bookname': doc.get('bookname'),
                        'authors': extract_authors(doc.get('authors')),
                        'publisher': doc.get('publisher'),
                        'publication_year': doc.get('publication_year'),
                        'isbn13': doc.get('isbn13'),
                        'addition_symbol': doc.get('addition_symbol'),
                        'vol': doc.get('vol'),
                        'class_no': doc.get('class_no'),
                        'class_nm': doc.get('class_nm'),
                        'bookImageURL': doc.get('bookImageURL'),
                        'bookDtlUrl': doc.get('bookDtlUrl'),
                        'loan_count': int(doc.get('loan_count', 0)) if doc.get('loan_count') else None
                    }
                    total_rows += 1
                    valid, err = Data4LibraryLoanItemValidator.validate_row(row)
                    if valid:
                        all_rows.append(row)
                    else:
                        fail_rows.append(row)
                        fail_msgs.append(f"id={row['id']} - {err}")
            except Exception as e:
                logger.error(f"파일 처리 실패: {file_path} - {e}")

        fail_count = len(fail_rows)
        success_count = len(all_rows)
        fail_rate = (fail_count / total_rows) * 100 if total_rows > 0 else 0
        logger.info(f"검증 결과: 전체 {total_rows}건, 성공 {success_count}건, 실패 {fail_count}건, 실패율 {fail_rate:.2f}%")
        if fail_count > 0:
            logger.warning(f"검증 실패 row 예시: {fail_msgs[:5]}")
        if fail_rate >= 70:
            logger.error(f"실패율 {fail_rate:.2f}%로 적재 중단!")
            raise Exception(f"적재 중단: 실패율 {fail_rate:.2f}%")
        elif fail_count > 0:
            logger.warning(f"실패율 {fail_rate:.2f}%: 일부 row만 적재 진행")

        # bulk insert
        if all_rows:
            try:
                cursor.executemany(insert_sql, all_rows)
                db.commit()
                logger.info(f"DB 적재 성공: {len(all_rows)}건")
            except Exception as e:
                logger.error(f"DB bulk insert 실패: {e}")
        else:
            logger.warning("적재할 데이터가 없습니다.")
        cursor.close()
        db.close()

        # 파일 압축
        logger.info(f"파일 압축 시작: {self.base_dir}/{self.endpoint}")
        save_date = pendulum.now().format('YYYYMMDD')
        zip_path = os.path.join(self.base_dir, self.endpoint, f"save_to_db_{save_date}.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in json_files:
                arcname = os.path.relpath(file_path, os.path.join(self.base_dir, self.endpoint))
                zipf.write(file_path, arcname)
        logger.info(f"압축 파일 생성 완료: {zip_path}")

        # (선택) 원본 파일 삭제/이동 등 후처리 필요시 여기에 추가
        logger.info("전체 작업 완료") 