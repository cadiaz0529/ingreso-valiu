import pandas as pd
from sqlalchemy import create_engine

MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]
MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_DATABASE = os.environ["MYSQL_DATABASE"]

mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8', pool_recycle=3600, encoding='utf-8')
mysql_connection = mysql_engine.connect()

##############################################
# Cash-ins
##############################################

def run_cashins_etl():
    cashins_query = """
    SELECT `ID`, `User ID`, `Created At`, `Amount Src`, `Amount Dst`
    FROM central_financial
    WHERE `Asset Src` = 'COP' AND `Asset Dst` = 'USDv' AND State = 'COMPLETED' AND Type = 'NORMAL'
    """
    cashins = pd.read_sql(cashins_query, mysql_connection)
    cashins.to_sql("cashins", mysql_connection, if_exists='replace', index=False)

##############################################
# Cash-outs
##############################################

def run_cashouts_etl():
    cashouts_query = """
    SELECT `ID`, `User ID`, `Created At`, `Amount Src`, `Amount Dst`
    FROM central_financial
    WHERE `Asset Src` = 'USDv' AND `Asset Dst` = 'VES' AND State = 'COMPLETED' AND Type = 'NORMAL'
    """
    cashouts = pd.read_sql(cashouts_query, mysql_connection)
    cashouts.to_sql("cashouts", mysql_connection, if_exists='replace', index=False)


if __name__ == "__main__":
    run_cashins_etl()
    run_cashouts_etl()