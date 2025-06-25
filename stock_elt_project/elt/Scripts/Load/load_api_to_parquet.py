import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def get_latest_file_in_directory(directory,extension):
    # Get a list of files in the directory with the specified extension
    files = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]

    # If there are no files in directory then return None
    if not files:
        return None
    
    # Find the latest files based on modification time
    latest_file = max(files,key=os.path.getmtime)
    return latest_file

def load_json_from_file(filepath):
    # Load JSON data from a file
    with open(filepath,'r',encoding='utf-8') as file:
        data = json.load(file)
    return data

def save_json_to_parquet(data,output_filepath):
    # Convert JSON data to a pyarrow Table
    table = pa.Table.from_pandas(pd.DataFrame(data))

    # Save the pyarrow table as a Parquet file
    pq.write_table(table,output_filepath)

def load_db_to_dl(input_directory,output_directory):
    extension = '.json'

    # Get the latest JSON file in the directory
    latest_file = get_latest_file_in_directory(input_directory,extension)

    if latest_file:
        # Read the JSON data
        data = load_json_from_file(latest_file)
        print(f'Read file: {latest_file}')

        # Set the Parquet file name corresponding to the JSON file name
        # Path to the output Parquet file
        filename = os.path.basename(latest_file).replace('.json','.parquet')
        output_filepath = os.path.join(output_directory,filename)

        # Save the JSON data as a Parquet file
        save_json_to_parquet(data,output_filepath)
        print(f'Saved Parquet file: {output_filepath}')
    else:
        print('No JSON files found in the directory')

def load_api_to_parquet():
    # Convert News JSON files to parquet
    # Path to the directory containing the JSON files
    input_directory = r'/home/thangtranquoc/stock_elt_project/elt/Data/raw/news'
    # Path to the directory to save the parquet files
    output_directory = r'/home/thangtranquoc/stock_elt_project/elt/Data/completed/load_api_news_to_dl'
    load_db_to_dl(input_directory,output_directory)

    # Convert OHLCs JSON files to parquet
    # Path to the directory containing the JSON files
    input_directory = r'/home/thangtranquoc/stock_elt_project/elt/Data/raw/ohlc'
    # Path to the directory to save the parquet files
    output_directory = r'/home/thangtranquoc/stock_elt_project/elt/Data/completed/load_api_ohlcs_to_dl'
    load_db_to_dl(input_directory,output_directory)

# load_api_to_parquet()