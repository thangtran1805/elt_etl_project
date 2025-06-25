import os
import pandas as pd
import datetime
from sqlalchemy import create_engine

# Read SQL query from a file
def read_query_from_a_file(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query

# Execute the query and save the result to a Parquet file using raw_connection
def query_to_parquet(query, engine, parquet_file_path):
    raw_conn = engine.raw_connection()
    try:
        df = pd.read_sql(query, raw_conn)
        print(df.info())
        print(df)
        df.to_parquet(parquet_file_path, engine='pyarrow')
    finally:
        raw_conn.close()

def load_db_to_parquet():
    # Connection params
    host = 'localhost'
    user = 'postgres'
    password = 'admin'
    port = '5432'
    database = 'datasource'

    # Create engine to connect to PostgreSQL
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

    # SQL query file path
    query_file_path = '/home/thangtranquoc/projects/stock_elt_project/elt/Scripts/Extract/extract_db_to_parquet.sql'

    # Output Parquet file path
    date = datetime.date.today().strftime('%Y_%m_%d')
    parquet_file_path = f'/home/thangtranquoc/projects/stock_elt_project/elt/Data/completed/load_db_to_dl/load_db_to_dl_{date}.parquet'

    # Read SQL query and write result to Parquet
    query = read_query_from_a_file(query_file_path)
    query_to_parquet(query, engine, parquet_file_path)
    print(f'Saved data from database to parquet successfully at {parquet_file_path}')

# load_db_to_parquet()
