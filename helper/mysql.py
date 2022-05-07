import pymysql.cursors
import settings


def get_conn(meta: dict, db: str):
    connection = pymysql.connect(host=meta['host'],
                                 port=meta['port'],
                                 user=meta['user'],
                                 password=meta['pwd'],
                                 database=db,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection


def get_binlog_info():
    conn = get_conn()

    with conn.cursor() as cursor:
        sql = 'show master status;'
        cursor.execute(sql)
        conn.commit()
        t = cursor.fetchone()
        return t['File'], t['Position']


def fetch_one(sql: str, conn):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
        t = cursor.fetchone()
        return t


def fetch(sql: str, conn):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
        for item in cursor.fetchall():
            yield item
