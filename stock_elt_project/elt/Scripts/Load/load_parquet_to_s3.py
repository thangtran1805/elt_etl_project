import boto3
import os
import datetime
from dotenv import load_dotenv

def get_latest_file_in_directory(directory,extension):
    """
        Get latest file in a directory with a specific extension.
        :param directory: A directory to search for file.
        :param extension: File extension to look for.
        :return: A path of latest file.
    """
    files = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files,key=os.path.getmtime)
    return latest_file

def load_parquet_to_s3():
    load_dotenv()
    extension = '.parquet'
    date = datetime.date.today().strftime('%Y_%m_%d')

    # S3 connection params
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('AWS_BUCKET_NAME')
    region = os.getenv('AWS_REGION')

    # S3 prefix and process news,ohlcs folder attributes
    folders = {
        'Process_news' : '/home/thangtranquoc/stock_elt_project/elt/Data/completed/load_api_news_to_dl',
        'Process_ohlcs' : '/home/thangtranquoc/stock_elt_project/elt/Data/completed/load_api_ohlcs_to_dl',
        'Process_companies' : '/home/thangtranquoc/stock_elt_project/elt/Data/completed/load_db_to_dl'
    }

    # S3 connection string
    s3 = boto3.client(
        's3',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        region_name = region
    )

    # Upload files upto prefix
    for prefix, folder in folders.items():
        file_path = get_latest_file_in_directory(folder,extension)
        if file_path:
            filename = os.path.basename(file_path)
            s3_key = f'{prefix}/{filename}'
            s3.upload_file(file_path,bucket_name,s3_key)
            print(f'Uploaded: {file_path} to s3://{bucket_name}/{s3_key}')
        else:
            print(f'No .parquet files were found! in {folder}')

# load_parquet_to_s3()