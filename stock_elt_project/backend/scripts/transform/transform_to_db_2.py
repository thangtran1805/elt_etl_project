import os
import json
import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine

def get_latest_file_in_directory(directory,extension):
    """
        Get the latest file in a directory with a specific extension.
        param: directory: A directory to search for file.
        param: extension: File extension to look for.
        :return: return None if no files were found.
    """
    files =[os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files,key=os.path.getmtime)
    return latest_file

def read_latest_file_in_directory(directory):
    """
        Read the JSON file in a directory.
        :param directory: A directory to search for file.
        :return: Data from the JSON file or an emty list if no files were found.
    """
    extension = '.json'
    latest_file = get_latest_file_in_directory(directory,extension)
    if latest_file:
        with open(latest_file,'r') as file:
            data_json = json.load(file)
        print(f'Transforming from file: {latest_file}')
    else:
        print('No file were found!')
        data_json = []
    return data_json

def cleaned_dataframe(dataframe):
    """
        Clean dataframe by replacing emty strings with Nan, dropping duplicates and dropping rows with Nan.
        :parm: dataframe: A dataframe to clean.
        :return: A cleaned dataframe.
    """
    return dataframe.replace(r'^\s*$',np.nan,regex=True).drop_duplicates().dropna()

def save_to_json(dataframe,filename):
    """
        Save a dataframe to a JSON file.
        :param dataframe: A dataframe to convert to JSON.
        :param filename: The path the the JSON file.
    """
    os.makedirs(os.path.dirname(filename),exist_ok=True)
    dataframe.to_json(filename,orient = 'records',lines=True)
    print(f'Saved dataframe to {filename}')

def transform_to_db_2():
    # Read the latest markets data
    markets = read_latest_file_in_directory('/home/thangtranquoc/projects/stock_elt_project/backend/data/raw/markets')

    # Get the current date for filenames
    date = datetime.date.today().strftime('%Y_%m_%d')

    # Transform markets data into exchanges Dataframe
    exchanges = cleaned_dataframe(pd.DataFrame([
        {
            'region' : item['region'],
            'primary_exchanges' : item['primary_exchanges']
        }
        for item in markets
    ]))


    # split primary_exchanges into mutiple rows
    exchanges = exchanges.assign(
        primary_exchanges = exchanges['primary_exchanges'].str.split(', ')
    )
    exchanges = exchanges.explode('primary_exchanges').reset_index(drop=True)

    # Database connect params
    username = 'postgres'
    password = 'admin'
    host = 'localhost'
    port = '5432'
    database = 'datasource'

    # Create database engine
    connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    # Load regions data from the database
    raw_conn = engine.raw_connection()
    try:
        regions = pd.read_sql_query("SELECT * FROM regions", raw_conn)
    finally:
        raw_conn.close()

    # Merge exchanges with regions
    exchanges = pd.merge(
        exchanges,
        regions,
        left_on = 'region',
        right_on = 'region_name'
    )[
        ['region_id','primary_exchanges']
    ]

    # Rename columns for better clarity
    new_columns = {'region_id':'exchange_region_id','primary_exchanges' : 'exchange_name'}
    exchanges.rename(columns=new_columns,inplace=True)

    # Save transformed exchanges data to JSON
    path = f'/home/thangtranquoc/projects/stock_elt_project/backend/data/processed/transformed_to_db_exchanges/process_exchanges_{date}.json'
    save_to_json(exchanges,path)

# transform_to_db_2()