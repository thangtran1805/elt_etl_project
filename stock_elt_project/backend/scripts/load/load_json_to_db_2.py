import psycopg2
import json
import os

def get_latest_file_in_directory(directory,extension):
    """
        Get the latest file in a directory with a specific extension.
        :param directory: A directory to search for file.
        :param extension: File extension to look for.
        :return: Path to the latest file and return None if no files were found.
    """
    files = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files,key=os.path.getmtime)
    return latest_file


def insert_data_from_json(file_path,table_name,columns,conflict_columns):
    """
     Insert data from JSON into PostgreSQL
     :param file_path: The path of the JSON file.
     :param table_name: Name of the table.
     :param columns: List of the columns to insert data into.
     :param coflict_columns: List of columns to check for conflict.
    """
    with open(file_path,'r') as file:
        data = [json.loads(line) for line in file]
    if not data:
        print(f'No data found in {file_path}')
        return 

    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    conflict_columns_str = ', '.join(conflict_columns)

    query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_columns_str}) DO NOTHING
"""
    conn = psycopg2.connect(
        host = 'localhost',
        user = 'postgres',
        password = 'admin',
        database = 'datasource'
 
   )
    cur = conn.cursor()

    for record in data:
        values = [record[col] for col in columns]
        cur.execute(query,values)

    conn.commit()
    cur.close()
    conn.close()
    print(f'Inserted data into {table_name}')

def load_json_to_db_2():
    # Define directory and table infomation
    directory = '/home/thangtranquoc/projects/stock_elt_project/backend/data/processed/transformed_to_db_exchanges'
    table_name = 'exchanges'
    columns = ['exchange_region_id','exchange_name']
    conflict_columns = ['exchange_name']

    # Get the latest file in the directory
    extension = '.json'
    latest_file = get_latest_file_in_directory(directory,extension)

    # Insert data from JSON into PostgreSQL
    if latest_file:
        insert_data_from_json(latest_file,table_name,columns,conflict_columns)
    else:
        print('No file were found to insert data!')

# load_json_to_db_2()