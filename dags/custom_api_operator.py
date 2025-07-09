# dags/fetch_books_dag.py
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from plugins.custom_api_operator import CustomAPIToFileOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="fetch_books",
    default_args=default_args,
    description="도서 대출 정보 수집",
    schedule_interval="@daily",
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["api", "library"]
) as dag:

    fetch_task = CustomAPIToFileOperator(
        task_id="fetch_loan_data",
        uri="http://data4library.kr/api",
        endpoint="loanItemSrch",
        params_list=[
            {"age": "10", "gender": "male"},
            {"age": "10", "gender": "female"},
            {"age": "20", "gender": "male"},
            {"age": "20", "gender": "female"},
            {"age": "30", "gender": "male"},
            {"age": "30", "gender": "female"},
            {"age": "40", "gender": "male"},
            {"age": "40", "gender": "female"},
            {"age": "50", "gender": "male"},
            {"age": "50", "gender": "female"},
            {"age": "60", "gender": "male"},
            {"age": "60", "gender": "female"},
        ],
        api_key=Variable.get("LIBRARY_API_KEY"),
        use_pagination=True,
        use_multithreading=True,
        response_format="json",
        base_save_path="data",
        max_workers=4
    )
