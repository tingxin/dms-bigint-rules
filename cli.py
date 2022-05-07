from settings import DATA_SOURCE, FOCUS_TYPE, COLUMN_LENGTH
import json
import click
import os
from datetime import datetime
from helper import db as dbk


data_source_name = [item for item in DATA_SOURCE.keys()]


@click.command("dms")
@click.option('-s', required=True, help='数据源名称', type=click.Choice(data_source_name))
@click.option('-d', required=True, help='db名称')
@ click.option('--exclude',  default="", prompt='需要排除的表名称，用,隔开多个需要排除的表名, 如果不需要排除，直接输入回车', help='需要排除的表名称，用,隔开多个需要排除的表名')
@click.option('--filename', default="", help='输出到结果到当前目录下，如果没有设置，默认输出到控制台')
def dms(s, d, exclude, filename):
    excludes = [item for item in exclude.strip().split(",") if item != ""]
    t = get_special_columns(s, d, excludes)
    rules = make_dms_rules(t, excludes, d)
    js = json.dumps(rules, sort_keys=True, indent=4, separators=(',', ':'))
    if filename == "":
        print(js)
    else:
        filepath = os.getcwd() + "/" + filename
        with open(filepath, mode='w') as f:
            f.write(js)


def make_dms_rules(columns_meta: list, excludes: list, db) -> dict:
    rules = list()
    ts = int(datetime.now().timestamp())
    index = 0
    if excludes:
        for item in columns_meta:
            key = str(ts + index)
            index += 1
            rule_selection = dict()
            rule_selection['rule-type'] = 'selection'
            rule_selection["rule-id"] = key
            rule_selection["rule-name"] = key
            rule_selection['object-locator'] = {
                "schema-name": db,
                "table-name": item['table']
            }
            rule_selection['rule-action'] = "include"
            rule_selection['filters'] = []

            rules.append(rule_selection)
    else:
        key = str(ts + index)
        index += 1
        rule_selection = dict()
        rule_selection['rule-type'] = 'selection'
        rule_selection["rule-id"] = key
        rule_selection["rule-name"] = key
        rule_selection['object-locator'] = {
            "schema-name": db,
            "table-name": "%"
        }
        rule_selection['rule-action'] = "include"
        rule_selection['filters'] = []

        rules.append(rule_selection)

    for item in columns_meta:
        for column in item["columns"]:
            key = str(ts + index)
            index += 1
            rule_transform = {
                "rule-type": "transformation",
                "rule-target": "column",
                "rule-id": key,
                "rule-name": key,
                "object-locator": {
                    "schema-name": db,
                    "table-name": item['table'],
                    "column-name": column
                },
                "rule-action": "change-data-type",
                "data-type": {
                    "type": "string",
                    "length": COLUMN_LENGTH
                }
            }
            rules.append(rule_transform)
    return {
        "rules": rules
    }


def get_special_columns(data_source: str, db: str, excludes: list) -> list:
    tables = list()
    conn = dbk.get_conn(datasource_key=data_source, db=db)
    for table in dbk.show_tables(data_source, db, conn=conn):
        if not excludes or table not in excludes:
            tables.append(table)

    result = list()
    for table in tables:
        entity = {
            "table": table,
            "columns": list()
        }

        for item in dbk.describe(datasource_key=data_source, db=db, table=table, conn=conn):
            if item[1] == FOCUS_TYPE:
                entity["columns"].append(item[0])

        result.append(entity)
    return result


if __name__ == '__main__':
    dms()
