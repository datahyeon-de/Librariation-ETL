from utils.dbutil import get_mysql_connection
import json
import csv

def load_default_loan_item_data(db_manager, table_name, **kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="transform_default_loan_item_data")
    
    columns = [
        "no", "ranking", "bookname", "authors", "publisher", "publication_year",
        "isbn13", "addition_symbol", "vol", "class_no", "class_nm",
        "bookImageURL", "bookDtlUrl", "loan_count", "start_dt", "end_dt"
    ]
    sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
    """
    
    with get_mysql_connection(db_manager) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, data)
            conn.commit()