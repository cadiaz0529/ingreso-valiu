import pandas as pd
from sqlalchemy import create_engine
import dateutil.parser as parser

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_DB = os.environ["POSTGRES_DB"]

postgres_engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
postgres_connection = postgres_engine.connect()

users_short = pd.read_csv("/etl/data/users_short.csv", encoding='utf8')

def run_etl():
    users_short["Created At"] = users_short["Created At"].map(parser.isoparse)
    users = users_short[["ID", "First Name", "Created At"]]

    users.to_sql("users", postgres_connection, if_exists='replace', index=False)

if __name__ == "__main__":
    run_etl()