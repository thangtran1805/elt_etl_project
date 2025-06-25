import os
import json
import pandas as pd
import numpy as np
import datetime

def get_latest_file_in_directory(directory,extension):
    files = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files,key=os.path.getmtime)
    return latest_file

def read_latest_file_in_directory(directory):
    extension = '.json'
    latest_file = get_latest_file_in_directory(directory,extension)
    if latest_file:
        with open(latest_file,'r') as file:
            data_json = json.load(file)
        print(f'Transforming from file: {latest_file}')
    else:
        print('No file found')
        data_json = []
    return data_json

def cleaned_dataframe(dataframe):
    return dataframe.replace(r'^\s*$',np.nan,regex=True).drop_duplicates().dropna()

def save_to_json(dataframe,filename):
    os.makedirs(os.path.dirname(filename),exist_ok=True)
    dataframe.to_json(filename,orient = 'records', lines = True)
    print(f'Saved dataframe to {filename}')

def transform_to_db_1():
    companies = read_latest_file_in_directory('/home/thangtranquoc/projects/stock_elt_project/backend/data/raw/companies')
    markets = read_latest_file_in_directory('/home/thangtranquoc/projects/stock_elt_project/backend/data/raw/markets')

    date = datetime.date.today().strftime('%Y_%m_%d')

    regions = cleaned_dataframe(pd.DataFrame([
        {
        'region_name' : item['region'],
        'region_local_open' : item['local_open'],
        'region_local_close' : item['local_close']
        }
        for item in markets
    ]))
    regions_path = f'/home/thangtranquoc/projects/stock_elt_project/backend/data/processed/transformed_to_db_regions/process_regions_{date}.json'
    save_to_json(regions,regions_path)

    industries = cleaned_dataframe(pd.DataFrame([
        {
            'industry_name' : item['industry'],
            'industry_sector' : item['sector']
        }
        for item in companies
    ]))
    industries_path = f'/home/thangtranquoc/projects/stock_elt_project/backend/data/processed/transformed_to_db_industries/process_industries_{date}.json'
    save_to_json(industries,industries_path)

    sicindustries = cleaned_dataframe(pd.DataFrame([
        {
            'sic_id' : item['sic'],
            'sic_industry' : item['sicIndustry'],
            'sic_sector' : item['sicSector']
        }
        for item in companies
    ]))
    sicindustries_path = f'/home/thangtranquoc/projects/stock_elt_project/backend/data/processed/transformed_to_db_sicindustries/process_sicindustries_{date}.json'
    save_to_json(sicindustries,sicindustries_path)

# transform_to_db_1()
