import os
import time
import argparse
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'
    os.system(f"wget {url} -O {csv_name}")
    engin = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df = pd.read_csv(csv_name, nrows=100)
    df.head(0).to_sql(name=table_name, con=engin, if_exists='replace')
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    total =0
    for df in df_iter:
        s_time = time.time()
        total += len(df)
        # check if the columns tppep_pickup_datetime and tpep_dropoff_datetime are in the dataframe then convert them to datetime
        if 'tpep_pickup_datetime' in df.columns and 'tpep_dropoff_datetime' in df.columns:
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engin, if_exists='append')
        e_time = time.time()
        print(f' insterted another chunck {e_time - s_time :0.2f} seconds, Total={total}')
        



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='posrt for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table to which the data will be ingested')
    parser.add_argument('--url', help='usrl of the csv file')
    args = parser.parse_args()
    main(args)
