import duckdb
import s3fs
from pyspark.sql import SparkSession
import pyarrow as pa
import os
from dotenv import load_dotenv

# JAVA config
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

# AWS credentials
load_dotenv()
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket = os.getenv('AWS_BUCKET_NAME')
s3_prefix = 'Process_companies'

def get_latest_parquet_file():
    fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_access_key)
    full_path = f"{s3_bucket}/{s3_prefix}"
    parquet_files = [f for f in fs.ls(full_path) if f.endswith('.parquet')]
    if not parquet_files:
        raise Exception("No Parquet files found in S3 path.")
    latest_file = max(parquet_files, key=lambda x: fs.info(x)['LastModified'])
    return f"s3a://{latest_file}"

def process(parquet_file_path):
    spark = SparkSession.builder \
        .appName('Insert Parquet into DuckDB') \
        .config('spark.jars', '/home/thangtranquoc/jars/hadoop-aws-3.3.4.jar,/home/thangtranquoc/jars/aws-java-sdk-bundle-1.11.1026.jar') \
        .config('spark.hadoop.fs.s3a.access.key', aws_access_key) \
        .config('spark.hadoop.fs.s3a.secret.key', aws_secret_access_key) \
        .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.fast.upload', 'true') \
        .config("spark.hadoop.yarn.resourcemanager.delegation-token-renewer.thread-timeout", "60000") \
        .config("spark.hadoop.yarn.resourcemanager.delegation-token-renewer.thread-retry-interval", "60000") \
        .config("spark.hadoop.yarn.router.subcluster.cleaner.interval.time", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000") \
        .getOrCreate()

    # Hadoop conf
    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.connection.timeout", "60000")
    hconf.set("fs.s3a.connection.request.timeout", "60000")
    hconf.set("fs.s3a.connection.establish.timeout", "60000")
    hconf.set("fs.s3a.threads.keepalivetime", "60000")

    print(f"📥 Đang đọc file: {parquet_file_path}")
    df_spark = spark.read.parquet(parquet_file_path)
    df_spark.printSchema()

    # Cột bắt buộc theo schema mới
    required_columns = [
        "company_name",
        "company_ticket",
        "company_is_delisted",
        "company_category",
        "company_currency",
        "company_location",
        "company_exchange_name",  
        "company_region_name",
        "company_industry_name",
        "company_industry_sector",
        "company_sic_industry",
        "company_sic_sector"
        ]
    missing_cols = [col for col in required_columns if col not in df_spark.columns]
    if missing_cols:
        raise ValueError(f"❌ Thiếu các cột bắt buộc trong Parquet: {missing_cols}")

    # Chuyển sang Arrow Table
    df_arrow = df_spark.select(*required_columns).toPandas()
    arrow_table = pa.Table.from_pandas(df_arrow)

    # Ghi vào DuckDB
    conn = duckdb.connect('/home/thangtranquoc/stock_elt_project/datawarehouse.duckdb')
    conn.register('arrow_table', arrow_table)
    conn.execute('''
        INSERT INTO dim_companies (
            company_name,
            company_ticket,
            company_is_delisted,
            company_category,
            company_currency,
            company_location,
            company_exchange_name,
            company_region_name,
            company_industry_name,
            company_industry_sector,
            company_sic_industry,
            company_sic_sector
        ) SELECT 
            company_name,
            company_ticket,
            company_is_delisted,
            company_category,
            company_currency,
            company_location,
            company_exchange_name,
            company_region_name,
            company_industry_name,
            company_industry_sector,
            company_sic_industry,
            company_sic_sector
        FROM arrow_table
    ''')
    conn.close()
    spark.stop()
    print("✅ Đã insert dữ liệu vào bảng dim_companies từ file Parquet.")

def transform_to_datawarehouse_1():
    parquet_file_path = get_latest_parquet_file()
    print("🔍 Latest Parquet File:", parquet_file_path)
    process(parquet_file_path)

# transform_to_datawarehouse_1()

