import pymysql

def get_mysql_connection(host: str, user: str, password: str, db: str):
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db,
        charset='utf8mb4',
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )