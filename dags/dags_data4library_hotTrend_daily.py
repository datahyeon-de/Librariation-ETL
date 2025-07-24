from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="dags_data4library_hotTrend_daily",
    start_date=pendulum.datetime(2025, 7, 22, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
    
) as dag:
    
    hot_trend_task = HttpOperator(
        task_id="hot_trend_task",
        http_conn_id="data4library.kr",
        endpoint="api/hotTrend/",
        method="GET",
        data={
            "authKey": "{{ var.value.LIBRARY_API_KEY }}",
            "searchDt": "{{ ds }}",
            "format": "json",
        },
        log_response=True,
    )
    
    @task(task_id="extract_hot_trend_data")
    def extract_hot_trend_data(**kwargs):
        ti = kwargs['ti']
        hot_trend_data = ti.xcom_pull(task_ids="hot_trend_task")
        
        import json
        from pprint import pprint
        
        if isinstance(hot_trend_data, str):
            pprint(json.loads(hot_trend_data))
        else:
            pprint(hot_trend_data)
        
    hot_trend_task >> extract_hot_trend_data()