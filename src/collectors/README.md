# Data Collectors

이 디렉토리는 다양한 데이터 소스에서 데이터를 수집하는 모듈들을 포함합니다.

## 구조
- `book_api_collector.py`: 도서 API 데이터 수집
- `library_crawler.py`: 도서관 웹사이트 크롤링
- `database_collector.py`: 데이터베이스에서 데이터 수집

## 사용법
각 수집기는 독립적으로 사용할 수 있으며, Airflow DAG에서 import하여 사용합니다. 