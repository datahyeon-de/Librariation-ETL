from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from concurrent.futures import ThreadPoolExecutor, as_completed
from plugins.utils.log_helper import get_logger
from plugins.utils.db_utils import get_mysql_connection
from plugins.utils.api_helper import find_key_value

import requests
import pendulum

class Data4LibraryUpdateBookDescriptionOperator(BaseOperator):
    def __init__(self, mysql_conn_id: str, log_level: str = None, **kwargs):
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.log_level = log_level

    def execute(self, context):
        # 실행 시간 및 로거 설정
        run_time = pendulum.now().format('YYYYMMDDTHHmmss')
        log_name = "update_book_description"
        log_dir = "/opt/airflow/files/data4library/archive/logs"
        logger = get_logger(log_name, log_dir, run_time, level=self.log_level, stream=True)
        
        # 작업 시간 측정을 위한 시작 시간 기록
        start_time = pendulum.now()
        logger.info(f"=== 도서 설명 업데이트 작업 시작: {start_time.format('YYYY-MM-DD HH:mm:ss')} ===")
        
        # DB 연결 변수 초기화
        logger.info(f"DB 연결 시도: conn_id={self.mysql_conn_id}")
        db = None
        cursor = None
        
        try:
            # DB 연결 설정
            conn = BaseHook.get_connection(self.mysql_conn_id)
            db = get_mysql_connection(conn.host, conn.login, conn.password, conn.schema)
            cursor = db.cursor()
            logger.info(f"DB 연결 성공: {conn.host}/{conn.schema}")
            
            # SQL 파일 읽기 및 데이터 조회
            sql_path = '/opt/airflow/sql/select_book_description.sql'
            try:
                with open(sql_path, 'r', encoding='utf-8') as f:
                    sql = f.read()
            except FileNotFoundError:
                logger.error(f"SQL 파일을 찾을 수 없습니다: {sql_path}")
                raise
            except Exception as e:
                logger.error(f"SQL 파일 읽기 실패: {sql_path} - {e}")
                raise
            
            logger.info("`book` 테이블에서 description이 NULL인 데이터 조회")
            cursor.execute(sql, (5000,))
            books = cursor.fetchall()
            logger.info(f"수집 대상 도서 수: {len(books)}")
            
            # 처리 대상 데이터 존재 여부 확인
            if not books:
                logger.warning("처리할 도서가 없습니다. 작업을 종료합니다.")
                total_duration = pendulum.now() - start_time
                logger.info(f"=== 작업 완료 (처리 대상 없음) - 소요시간: {total_duration.total_seconds():.2f}초 ===")
                return
            
            # API 설정 및 연결 정보
            api_key = Variable.get("LIBRARY_API_KEY")
            logger.info("API 키 로드 완료")
            
            http_conn_id = "data4library.kr"
            connection = BaseHook.get_connection(http_conn_id)
            endpoint = "srchDtlList"
            base_url = f"http://{connection.host}:{connection.port}/api"
            full_url = f"{base_url}/{endpoint}"
            
            # API 호출 함수 정의
            def _api_call(book):
                # 도서 정보 추출
                isbn13 = book.get('isbn13', 'unknown')
                book_id = book.get('book_id', 'unknown')
                
                try:
                    # API 요청 파라미터 설정
                    params = {
                        "authKey": api_key,
                        "isbn13": isbn13,
                        "format": "json"
                    }
                    
                    # API 호출 및 응답 처리
                    response = requests.get(full_url, params=params)
                    logger.debug(f"API 요청 URL: {response.url}")
                    response.raise_for_status()
                    
                    # 응답 데이터에서 description 추출
                    data = response.json()
                    description, found = find_key_value(data, "description")
                    
                    if not found:
                        logger.warning(f"도서 설명을 찾을 수 없음: {isbn13}")
                        return None
                    
                    logger.info(f"도서 정보 수집 성공: {isbn13}")
                    return (book_id, description)
                
                except Exception as e:
                    logger.error(f"API 호출 실패: {isbn13} - err:{e}")
                    return None

            # 병렬 API 호출 실행
            api_start_time = pendulum.now()
            logger.info(f"API 호출 시작: {len(books)}개 도서 처리 예정")
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(_api_call, book) for book in books]
                results = []
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Future 결과 처리 중 오류: {e}")
                        results.append(None)
            
            # API 호출 결과 통계 계산
            api_end_time = pendulum.now()
            api_duration = api_end_time - api_start_time
            successful_results = [r for r in results if r is not None]
            failed_count = len(books) - len(successful_results)
            success_rate = (len(successful_results) / len(books)) * 100 if books else 0
            
            logger.info(f"API 호출 완료 - 소요시간: {api_duration.total_seconds():.2f}초")
            logger.info(f"성공: {len(successful_results)}개, 실패: {failed_count}개, 성공률: {success_rate:.1f}%")
            
            # 성공한 결과가 없는 경우 DB 업데이트 건너뛰기
            if not successful_results:
                logger.warning("성공한 API 호출이 없습니다. DB 업데이트를 건너뜁니다.")
                total_duration = pendulum.now() - start_time
                logger.info(f"=== 작업 완료 (API 성공 없음) - 총 소요시간: {total_duration.total_seconds():.2f}초 ===")
                return
            
            # 데이터베이스 업데이트 SQL 로드
            update_sql_path = '/opt/airflow/sql/update_book_description.sql'
            try:
                with open(update_sql_path, 'r', encoding='utf-8') as f:
                    update_sql = f.read()
            except FileNotFoundError:
                logger.error(f"UPDATE SQL 파일을 찾을 수 없습니다: {update_sql_path}")
                raise
            except Exception as e:
                logger.error(f"UPDATE SQL 파일 읽기 실패: {update_sql_path} - {e}")
                raise
            
            # 데이터베이스 업데이트 실행
            db_update_start = pendulum.now()
            logger.info("데이터베이스 업데이트 시작")
            
            updated_count = 0
            for result in successful_results:
                book_id, description = result
                try:
                    cursor.execute(update_sql, (description[:1000], book_id))
                    updated_count += 1
                    logger.debug(f"도서 업데이트 완료: book_id={book_id}")
                except Exception as e:
                    logger.error(f"DB 업데이트 실패: book_id={book_id}, err={e}")
                    continue  # 다음 항목 계속 처리
            
            # 최종 작업 완료 통계
            db_update_end = pendulum.now()
            db_duration = db_update_end - db_update_start
            total_duration = db_update_end - start_time
            
            logger.info(f"DB 업데이트 완료 - 소요시간: {db_duration.total_seconds():.2f}초")
            logger.info(f"총 {updated_count}개 도서 설명 업데이트 완료")
            logger.info(f"=== 전체 작업 완료 - 총 소요시간: {total_duration.total_seconds():.2f}초 ===")
        
        except Exception as e:
            logger.error(f"작업 실행 중 오류 발생: {e}")
            raise e
        
        finally:
            # DB 리소스 정리
            if cursor:
                cursor.close()
                logger.debug("DB 커서 해제 완료")
            if db:
                db.close()
                logger.debug("DB 연결 해제 완료")