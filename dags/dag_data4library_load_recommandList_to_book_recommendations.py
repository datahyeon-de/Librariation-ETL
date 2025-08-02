from airflow import DAG
from plugins.operators.load_recommandList_to_book_recommendations import BookRecommendationsFileSaveToDBOperator
import pendulum

with DAG(
    dag_id='dag_data4library_load_recommandList_to_book_recommendations',
    description='data4library - recommandList 수집 데이터를 book_recommendations 테이블에 적재',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 8, 2, tz="Asia/Seoul"),
    catchup=False,
    tags=['recommandList', 'load',],
) as dag:

    task_load_recommandList_to_book_recommendations = BookRecommendationsFileSaveToDBOperator(
        task_id='task_load_recommandList_to_book_recommendations',
        endpoint='recommandList',
        mysql_conn_id='librariation_dev',  
        base_dir='/opt/airflow/files/data4library',
    )