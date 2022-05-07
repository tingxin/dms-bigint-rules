import pymssql
import settings


def get_conn(meta: dict, db: str):
    connection = pymssql.connect(meta['host'],
                                 meta['user'],
                                 meta['pwd'],
                                 db)
    return connection


def fetch(sql: str, conn):
    cursor = conn.cursor()
    cursor.execute(sql)
    for row in cursor:
        yield row


def execute(sql: str, conn):
    cursor = conn.cursor()
    err = cursor.execute(sql)
    print(err)
    t = conn.commit()
    print(t)
