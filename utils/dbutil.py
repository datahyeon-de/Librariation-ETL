import pymysql
from pymysql.cursors import DictCursor

def get_mysql_connection(db_manager):
    """
    DBSettingManager 인스턴스를 받아 pymysql 커넥션을 반환
    """
    db_config = db_manager.get_db_config()
    connection = pymysql.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        charset="utf8mb4",
        cursorclass=DictCursor
    )
    return connection 