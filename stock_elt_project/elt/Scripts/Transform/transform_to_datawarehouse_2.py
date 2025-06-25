import duckdb
import s3fs
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pyarrow as pa
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# JAVA config
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

# AWS credentials
load_dotenv()
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket = os.getenv('AWS_BUCKET_NAME')
s3_prefix = 'Process_ohlcs'

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
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.hadoop.yarn.resourcemanager.delegation-token-renewer.thread-timeout", "60000") \
        .config("spark.hadoop.yarn.resourcemanager.delegation-token-renewer.thread-retry-interval", "60000") \
        .config("spark.hadoop.yarn.router.subcluster.cleaner.interval.time", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000") \
        .getOrCreate()

    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.connection.timeout", "60000")
    hconf.set("fs.s3a.connection.request.timeout", "60000")
    hconf.set("fs.s3a.connection.establish.timeout", "60000")
    hconf.set("fs.s3a.threads.keepalivetime", "60000")

    print(f"📥 Đang đọc file: {parquet_file_path}")
    df_spark = spark.read.option("mergeSchema", "false").load(parquet_file_path, format="parquet")

    # Rename columns trước khi dùng, tránh bị trùng tên
    df_spark = df_spark \
        .withColumnRenamed("T", "company_ticket") \
        .withColumnRenamed("v", "volume") \
        .withColumnRenamed("vw", "volume_weighted") \
        .withColumnRenamed("o", "open") \
        .withColumnRenamed("c", "close") \
        .withColumnRenamed("h", "high") \
        .withColumnRenamed("l", "low") \
        .withColumnRenamed("t", "time_stamp") \
        .withColumnRenamed("n", "num_of_trades") \
        .withColumnRenamed("otc", "is_otc")

    # Chỉ giữ các cột đã rename để loại bỏ các cột duplicate nếu Spark giữ hidden meta fields
    df_spark = df_spark.select(
        "company_ticket", "volume", "volume_weighted", "open", "close",
        "high", "low", "time_stamp", "num_of_trades", "is_otc"
    )

    df_spark.printSchema()
    df_spark.show()

    yesterday = datetime.today().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")

    conn = duckdb.connect('/home/thangtranquoc/projects/stock_elt_project/datawarehouse.duckdb')

    conn.execute(f'''
        INSERT INTO dim_time (date, day_of_week, month,quater, year)
        SELECT 
            '{yesterday}',
            '{yesterday.strftime('%A')}',
            '{yesterday.strftime('%B')}',
            {((yesterday.month - 1) // 3) + 1},
            {yesterday.year}
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_time WHERE date = '{yesterday}'
        )
    ''')

    time_id = conn.execute(f"SELECT time_id FROM dim_time WHERE date = '{yesterday}'").fetchone()[0]

    id_company_df = conn.execute("SELECT company_id, company_ticket FROM dim_companies").fetchdf()
    companies_df = id_company_df.drop_duplicates(subset=['company_ticket'], keep='last')
    companies_spark_df = spark.createDataFrame(companies_df)

    df_spark = df_spark.join(companies_spark_df, on='company_ticket', how='left')
    df_spark = df_spark.filter(df_spark['company_id'].isNotNull())
    df_spark = df_spark.withColumn('candles_time_id', lit(time_id))

    # Cột cần để insert
    expected_cols = [
        'volume', 'volume_weighted', 'open', 'close', 'high', 'low',
        'time_stamp', 'num_of_trades', 'is_otc', 'candles_time_id', 'company_id'
    ]
    df_spark = df_spark.select(*expected_cols)

    arrow_table = pa.Table.from_pandas(df_spark.toPandas())
    conn.register('arrow_table', arrow_table)

    conn.execute('''
        INSERT INTO fact_candles (
            candle_volume,
            candle_volume_weighted,
            candle_open,
            candle_close,
            candle_high,
            candle_low,
            candle_time_stamp,  
            candle_num_of_trades,
            candle_is_otc,
            candles_time_id,
            candle_company_id
        )
        SELECT 
            volume,
            volume_weighted,
            open,
            close,
            high,
            low,
            time_stamp,
            num_of_trades,
            is_otc,
            candles_time_id,
            company_id
        FROM arrow_table
    ''')

    print("✅ Đã insert dữ liệu vào bảng fact_candles từ file Parquet.")
    conn.close()
    spark.stop()

def transform_to_datawarehouse_2():
    parquet_file_path = get_latest_parquet_file()
    print("🔍 Latest Parquet File:", parquet_file_path)
    process(parquet_file_path)

# transform_to_datawarehouse_2()
