import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def get_the_latest_file_in_directory(directory,extension):
    """
        Get the latest file in a directory with a specific extension
        :param directory: A directory to search for files.
        :param extension: File extension to look for.
        :return: Path to the latest file or return None if no files were found!
    """
    files = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files,key=os.path.getmtime)
    return latest_file

def load_json_from_file(filepath):
    """
        Load JSON data from a file.
    """
    with open(filepath,'r') as file:
        data = [json.loads(line) for line in file if line.strip()]
    return data

def save_json_to_parquet(data,output_filepath):
    # Convert JSON data to a pyarrow table
    table = pa.Table.from_pandas(pd.DataFrame(data))

    # Save the pyarrow table as a Parquet file
    pq.write_table(table,output_filepath)

def load_db_to_dl(input_directory,output_directory):
    extension = '.json'

    # Get the latest JSON file in a directory
    latest_file = get_the_latest_file_in_directory(input_directory,extension)

    if latest_file:
        data = load_json_from_file(latest_file)
        print(f'Read file: {latest_file}')

        # Set the parquet file name corresponding to the JSON file name
        # Path to the output Parquet file
        filename = os.path.basename(latest_file).replace('.json','.parquet')
        output_filepath = os.path.join(output_directory,filename)

        # Save the JSON data as a Parquet file
        save_json_to_parquet(data,output_filepath)
        print(f'Saved Parquet file: {output_filepath}')
    else:
        print('No files were found in directory')

def load_companies_to_parquet():
    # Convert the companies JSON file to parquet
    # Path to the directory containing the JSON file
    input_directory = r'/home/thangtranquoc/projects/stock_elt_project/backend/data/processed/transformed_to_db_companies'
    # Path to the directory to save the Parquet file
    output_directory = r'/home/thangtranquoc/projects/stock_elt_project/backend/data/processed/load_companies_to_dw'
    load_db_to_dl(input_directory,output_directory)

# load_companies_to_parquet()
