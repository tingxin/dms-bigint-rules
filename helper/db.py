from settings import DATA_SOURCE
from helper import mssql, mysql

t = f"datasource_key should be in {[item for item in DATA_SOURCE.keys()]}"


def get_conn(datasource_key: str, db: str):
    db_meta = DATA_SOURCE[datasource_key]
    if db_meta['source_type'] == 'mysql':
        return mysql.get_conn(db_meta, db)

    elif db_meta['source_type'] == 'mssql':
        return mssql.get_conn(db_meta, db)
    else:
        raise ValueError(t)


def fetch(datasource_key: str, db: str, sql: str, conn):
    db_meta = DATA_SOURCE[datasource_key]
    if db_meta['source_type'] == 'mysql':
        return mysql.fetch(sql, conn=conn)

    elif db_meta['source_type'] == 'mssql':
        return mssql.fetch(sql, conn=conn)
    else:
        raise ValueError(t)


def describe(datasource_key: str, db: str, table: str, conn):
    db_meta = DATA_SOURCE[datasource_key]

    if db_meta['source_type'] == 'mysql':
        command = f'describe `{table}`'
        for row in mysql.fetch(command, conn=conn):
            yield row['Field'], row['Type']

    elif db_meta['source_type'] == 'mssql':
        command = f"EXEC sp_columns '{table}'"
        for row in mssql.fetch(command, conn=conn):
            yield row[3], row[5]
    else:
        raise ValueError(t)


def show_tables(datasource_key: str, db: str, conn):
    db_meta = DATA_SOURCE[datasource_key]

    if db_meta['source_type'] == 'mysql':
        command = f'show tables'
        key = f'Tables_in_{db}'
        for row in mysql.fetch(command, conn=conn):
            yield row[key]

    elif db_meta['source_type'] == 'mssql':
        command = f"SELECT * FROM INFORMATION_SCHEMA.TABLES"
        for row in mssql.fetch(command, conn=conn):
            yield row[2]
    else:
        raise ValueError(t)
