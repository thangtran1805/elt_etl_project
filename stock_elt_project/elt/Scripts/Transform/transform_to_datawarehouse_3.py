import duckdb
import s3fs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,input_file_name,lit
import pyarrow as pa
import os
from datetime import datetime,timedelta
from dotenv import load_dotenv

# JAVA config
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']

# AWS credentials
load_dotenv()
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket = os.getenv('AWS_BUCKET_NAME')
s3_prefix = 'Process_news'

def get_latest_parquet_file():
    fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_access_key)
    full_path = f"{s3_bucket}/{s3_prefix}"
    parquet_files = [f for f in fs.ls(full_path) if f.endswith('.parquet')]
    if not parquet_files:
        raise Exception("No Parquet files found in S3 path.")
    latest_file = max(parquet_files, key=lambda x: fs.info(x)['LastModified'])
    return f"s3a://{latest_file}"

def process(parquet_file_path):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Insert Parquet into DuckDB (dim_times, dim_topics, dim_news, fact_news_topics, fact_news_companies)") \
        .config("spark.sql.caseSensitive", "true") \
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

    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.connection.timeout", "60000")
    hconf.set("fs.s3a.connection.request.timeout", "60000")
    hconf.set("fs.s3a.connection.establish.timeout", "60000")
    hconf.set("fs.s3a.threads.keepalivetime", "60000")

    print(f"📥 Đang đọc file: {parquet_file_path}")

    # Connect to DuckDB
    database_path = '/home/thangtranquoc/projects/stock_elt_project/datawarehouse.duckdb'
    conn = duckdb.connect(database=database_path)
    
    # Read Parquet file into Spark DataFrame
    df_spark = spark.read.parquet(parquet_file_path)
    
    # Display schema and a few rows of data
    df_spark.printSchema()
    df_spark.show()
    
    # Step 1: Create DataFrame for dim_topics and insert new topics if they do not exist
    df_topics = df_spark.select(explode(col("topics")).alias("topic")) \
        .select("topic.topic").distinct().withColumnRenamed("topic", "topic_name")

    # Convert Spark DataFrame to Arrow Table
    arrow_table_topics = df_topics.toPandas()

    print(arrow_table_topics)

    # Insert data into dim_topics
    conn.register('arrow_table_topics', arrow_table_topics)
    conn.execute("""
        INSERT INTO dim_topics (topic_name)
        SELECT * FROM arrow_table_topics
        WHERE topic_name NOT IN (SELECT topic_name FROM dim_topics)
    """)
    print("Data inserted into dim_topics successfully!")
    
    # Step 2: Get yesterday's date and insert new time data into dim_time if it does not exist
    yesterday = datetime.now().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")
    conn.execute(f'''
        INSERT INTO dim_time (date, day_of_week, month, quater, year)
        SELECT
            '{yesterday}',
            '{yesterday.strftime("%A")}',
            '{yesterday.strftime("%B")}',
            '{((yesterday.month - 1) // 3) + 1}',
            {yesterday.year}
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_time WHERE date = '{yesterday}'
        )
    ''')
    
    # Get corresponding time_id from dim_time
    id_time_df = conn.execute(f'''
        SELECT time_id FROM dim_time WHERE date = '{yesterday}'
    ''').fetchdf()
        
    news_time_id = id_time_df['time_id'][0]

    # Step 3: Create DataFrame for dim_news
    df_news = df_spark.select(
        col("title").alias("new_title"),
        col("url").alias("new_url"),
        col("time_published").alias("new_time_published"),
        col("authors").alias("new_authors"),
        col("summary").alias("new_summary"),
        col("source").alias("new_source"),
        col("overall_sentiment_score").alias("new_overall_sentiment_score"),
        col("overall_sentiment_label").alias("new_overall_sentiment_label")
    ).withColumn("news_time_id", lit(news_time_id))
    
    # Convert Spark DataFrame to Arrow Table
    arrow_table_news = df_news.toPandas()
    
    print(arrow_table_news)

    # Step 4: Insert DataFrame dim_news into dim_news table
    conn.register('arrow_table_news', arrow_table_news)
    conn.execute('''
        INSERT INTO dim_news (
            new_title,
            new_url,
            new_time_published,
            new_authors,
            new_summary,
            new_source,
            new_overall_sentiment_score,
            new_overall_sentiment_label,
            news_time_id
        ) SELECT 
            new_title,
            new_url,
            new_time_published,
            new_authors,
            new_summary,
            new_source,
            new_overall_sentiment_score,
            new_overall_sentiment_label,
            news_time_id
        FROM arrow_table_news
    ''')
    print("Data inserted into dim_news successfully!")
    
    # Step 5: Create DataFrame for fact_news_topics
    df_fact_news_topics = df_spark.select(
        explode(col("topics")).alias("topic"),
        col("title").alias("new_title")
    ).select(
        col("topic.relevance_score").alias("new_topic_relevance_score"),
        col("topic.topic").alias("topic_name"),
        col("new_title")
    )
    arrow_table_fact_news_topics = df_fact_news_topics.toPandas()
    
    # Get corresponding topic_id from dim_topics
    id_topic_df = conn.execute('SELECT * FROM dim_topics').fetchdf()
    arrow_table_fact_news_topics = arrow_table_fact_news_topics.merge(id_topic_df, on='topic_name', how='left')
    arrow_table_fact_news_topics = arrow_table_fact_news_topics[arrow_table_fact_news_topics['topic_id'].notnull()]
    
    # Get corresponding new_id from dim_news
    id_new_df = conn.execute('SELECT new_id, new_title FROM dim_news').fetchdf()
    arrow_table_fact_news_topics = arrow_table_fact_news_topics.merge(id_new_df, on='new_title', how='left')
    arrow_table_fact_news_topics = arrow_table_fact_news_topics[arrow_table_fact_news_topics['new_id'].notnull()]

    print(arrow_table_fact_news_topics)
    
    # Load DataFrame into fact_news_topics table
    conn.register('arrow_table_fact_news_topics', arrow_table_fact_news_topics)
    conn.execute('''
        INSERT INTO fact_news_topics (
            new_topic_new_id,
            new_topic_topic_id,
            new_topic_relevance_score
        ) SELECT 
            new_id,
            topic_id,
            new_topic_relevance_score
        FROM arrow_table_fact_news_topics
    ''')
    print("Data inserted into fact_news_topics successfully!")
    
    # Step 6: Create DataFrame for fact_news_companies
    df_fact_news_companies = df_spark.select(
        explode(col("ticker_sentiment")).alias("ticker_sentiment"),
        col("title").alias("new_title")
    ).select(
        col("ticker_sentiment.relevance_score").alias("new_company_relevance_score"),
        col("ticker_sentiment.ticker").alias("company_ticket"),
        col("ticker_sentiment.ticker_sentiment_score").alias("new_company_ticker_sentiment_score"),
        col("ticker_sentiment.ticker_sentiment_label").alias("new_company_ticker_sentiment_label"),
        col("new_title")
    )
    arrow_table_fact_news_companies = df_fact_news_companies.toPandas()
    
    # Get corresponding new_id from dim_news
    arrow_table_fact_news_companies = arrow_table_fact_news_companies.merge(id_new_df, on='new_title', how='left')
    arrow_table_fact_news_companies = arrow_table_fact_news_companies[arrow_table_fact_news_companies['new_id'].notnull()]
    
    # Get corresponding company_id from dim_companies
    query = """
        SELECT company_id, company_ticket, company_time_stamp
        FROM (
            SELECT 
                company_id, 
                company_ticket, 
                company_time_stamp,
                ROW_NUMBER() OVER (PARTITION BY company_ticket ORDER BY company_time_stamp DESC) as row_num
            FROM dim_companies
        ) subquery
        WHERE row_num = 1;
    """
    id_company_df = conn.execute(query).fetchdf()
    arrow_table_fact_news_companies = arrow_table_fact_news_companies.merge(id_company_df, on='company_ticket', how='left')
    arrow_table_fact_news_companies = arrow_table_fact_news_companies[arrow_table_fact_news_companies['company_id'].notnull()]
    
    print(arrow_table_fact_news_companies)

    # Load DataFrame into fact_news_companies table
    conn.register('arrow_table_fact_news_companies', arrow_table_fact_news_companies)
    conn.execute('''
        INSERT INTO fact_news_companies (
            new_company_company_id,
            new_company_new_id,
            new_company_relevance_score,
            new_company_ticker_sentiment_score,
            new_company_ticker_sentiment_label
        ) SELECT 
            company_id,
            new_id,
            new_company_relevance_score,
            new_company_ticker_sentiment_score,
            new_company_ticker_sentiment_label
        FROM arrow_table_fact_news_companies
    ''')
    print("Data inserted into fact_news_companies successfully!")
    
    # Close DuckDB connection
    conn.close()
    
    # Stop Spark session
    spark.stop()

def transform_to_datawarehouse_3():
    parquet_file_path = get_latest_parquet_file()
    print("🔍 Latest Parquet File:", parquet_file_path)
    process(parquet_file_path)

# transform_to_datawarehouse_3()

