from airflow import DAG
from datetime import datetime
from plugins.operators.data4library_api_to_csv_operator import Data4LibraryAPItoCSVOperator

with DAG(
    dag_id="dags_data4library_loanItemSrch_daily",
    start_date=datetime(2025, 7, 22),
    schedule_interval="* 1 * * *",
    catchup=False,
) as dag:

    # 파라미터 예시: 성별, 연령, 지역 등 여러 값 조합
    params = {
        "gender": [0, 1, 2],
        "age": ["0", "6", "8", "14", "20", "30", "40", "50", "60"],
        "pageSize": [5000],
        "startDt": ["2025-07-21"],
        "endDt": ["2025-07-21"],
    }

    save_loanItemSrch_to_csv = Data4LibraryAPItoCSVOperator(
        task_id="save_loanItemSrch_to_csv",
        dataset_nm="loanItemSrch",
        path="/opt/airflow/files",  # 저장할 루트 경로
        file_name="loanItemSrch", # 파일명(확장자 없이, 내부에서 조합됨)
        api_params=params,         # 여러 값 조합 가능
        use_pagination=False,   # 페이지네이션 사용 여부
        use_multithreading=True, # 멀티스레딩 사용 여부
        max_workers=7
    )