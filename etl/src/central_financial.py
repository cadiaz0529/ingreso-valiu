import os
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import pymysql.cursors
import dateutil.parser as parser

MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]
MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_DATABASE = os.environ["MYSQL_DATABASE"]


mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}?charset=utf8', pool_recycle=3600, encoding='utf-8')
mysql_connection = mysql_engine.connect()

central_financial = pd.read_csv("/etl/data/central_financial.csv", encoding='latin-1')

central_financial["Created At"] = central_financial["Created At"].map(parser.isoparse)
central_financial["Sync Date"] = central_financial["Sync Date"].map(parser.isoparse)
central_financial["Updated At"] = central_financial["Updated At"].map(parser.isoparse)

central_financial_schema = [
    { "name": "ID", "type": "VARCHAR(50)" },
    { "name": "ID.1", "type": "VARCHAR(50)" },
    { "name": "Account ID Dst", "type": "VARCHAR(50)" },
    { "name": "Account ID Src", "type": "VARCHAR(50)" },
    { "name": "Amount Dst", "type": "DECIMAL(21, 8)" },
    { "name": "Amount Dst Usd", "type": "DECIMAL(21, 8)" },
    { "name": "Amount Src", "type": "DECIMAL(21, 8)" },
    { "name": "Amount Src Usd", "type": "DECIMAL(21, 8)" },
    { "name": "Asset Dst", "type": "VARCHAR(10)" },
    { "name": "Asset Src", "type": "VARCHAR(10)" },
    { "name": "Contact Dst", "type": "VARCHAR(50)" },
    { "name": "Created At", "type": "DATETIME" },
    { "name": "Description", "type": "VARCHAR(255)", "charset": "utf8" },
    { "name": "Order ID", "type": "VARCHAR(50)" },
    { "name": "Service Name", "type": "VARCHAR(50)", "charset": "utf8" },
    { "name": "Short ID", "type": "VARCHAR(50)" },
    { "name": "State", "type": "VARCHAR(20)" },
    { "name": "Sync Date", "type": "DATETIME" },
    { "name": "Type", "type": "VARCHAR(50)" },
    { "name": "Updated At", "type": "DATETIME" },
    { "name": "User ID", "type": "VARCHAR(50)" },
    { "name": "User Type", "type": "VARCHAR(20)" },
    { "name": "V", "type": "VARCHAR(10)" },
    { "name": "Wallet Dst", "type": "VARCHAR(50)" },
    { "name": "Wallet Src", "type": "VARCHAR(50)" }
]

def build_create_table_statement(tablename, schema, rdbms):
    cols_definition = []
    quote_char = "`" if rdbms == "mysql" else '"'
    for col in schema:
        charset = " CHARSET " + col["charset"] if rdbms == "mysql" and "charset" in col else ""
        col_definition = quote_char + col["name"] + quote_char + " " + col["type"] + charset
        cols_definition.append(col_definition)
    statement = f"CREATE TABLE {tablename} ({', '.join(cols_definition)})"
    return statement

def run_etl():
    connection = pymysql.connect(host=MYSQL_HOST,
                                user=MYSQL_USER,
                                password=MYSQL_PASSWORD,
                                database=MYSQL_DB,
                                cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS central_financial")
            create_statement = build_create_table_statement("central_financial", central_financial_schema, "mysql")
            cursor.execute(create_statement)
        connection.commit()

    central_financial.to_sql("central_financial", dbConnection, if_exists='append', index=False)


if __name__ == "__main__":
    run_etl()
