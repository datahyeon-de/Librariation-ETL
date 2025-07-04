# Airflow DAGs

이 디렉토리는 Airflow DAG 정의 파일들을 포함합니다.

## 구조
- `book_collection_dag.py`: 도서 데이터 수집 DAG
- `data_processing_dag.py`: 데이터 처리 DAG
- `data_validation_dag.py`: 데이터 검증 DAG

## 사용법
각 DAG는 src/ 디렉토리의 모듈들을 import하여 사용합니다. 