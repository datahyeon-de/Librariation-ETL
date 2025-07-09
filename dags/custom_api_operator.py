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
            {"age": "10", "gender": "male", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "10", "gender": "female", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "20", "gender": "male", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "20", "gender": "female", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "30", "gender": "male", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "30", "gender": "female", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "40", "gender": "male", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "40", "gender": "female", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "50", "gender": "male", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "50", "gender": "female", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "60", "gender": "male", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
            {"age": "60", "gender": "female", "pageSize": 10, "startDt": "{{ macros.ds_add(ds, -7) }}", "endDt": "{{ ds }}"},
        ],
        api_key=Variable.get("LIBRARY_API_KEY"),
        use_pagination=False,
        use_multithreading=True,
        response_format="json",
        base_save_path="data",
        max_workers=4
    )
