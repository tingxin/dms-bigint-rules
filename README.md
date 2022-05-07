# 项目背景
由于aws dms对BIGINT 暂不支持，数据迁移时，BIGINT 的字段会缺失精度，所以考虑通过table mapping 的方案先把字段都改成string 类型，在后续的使用方进行转换

手动配置table mapping ,如果表很多，或者字段很多，比较麻烦，该项目帮你自动生成这些转换规则

目前暂时只支持MYSQL 和SQL Server 数据源

## 使用
pip3 install -r requirement.txt
python3 cli.py --help

## 注意
请先配置settings.py文件中的 DATA_SOURCE,结构说明如下

```
{
    # 数据源的key
    'demo_cluster': {
        'source_type': 'mysql', # 数据源类型
        'host': 'demo.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn', 
        'port': 3306,
        'user': 'admin',
        'pwd': 'Demo1234'
    },
    'demo_mssql': {
        'source_type': 'mssql',
        'host':  'database-1.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn',
        'port': 1433,
        'user': 'admin',
        'pwd': 'Demo1234'
    }

}

```