import psycopg2
import json
import os

def get_the_latest_file_in_directory(directory, extension):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def insert_data_from_json(file_path, table_name, columns, conflict_columns):
    with open(file_path, 'r') as file:
        data = [json.loads(line) for line in file]
    if not data:
        print(f'No data found in {file_path}')
        return 

    # Lọc bỏ record nào thiếu các cột bắt buộc
    required_columns = ['company_exchange_id', 'company_industry_id', 'company_sic_id']
    filtered_data = []
    for record in data:
        if all(record.get(col) is not None for col in required_columns):
            filtered_data.append(record)

    if not filtered_data:
        print(f'❌ No valid records with required fields in {file_path}')
        return

    print(f"🧹 Filtered valid records: {len(filtered_data)} / {len(data)}")

    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    conflict_columns_str = ', '.join(conflict_columns)

    query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_columns_str}) DO NOTHING
    """

    conn = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='admin',
        database='datasource'
    )
    cur = conn.cursor()

    for record in filtered_data:
        values = [record.get(col) for col in columns]
        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()
    print(f'✅ Inserted {len(filtered_data)} records into {table_name}')

def load_json_to_db_3():
    directory = '/home/thangtranquoc/projects/stock_elt_project/backend/data/processed/transformed_to_db_companies'
    extension = '.json'
    table_name = 'companies'

    latest_file = get_the_latest_file_in_directory(directory, extension)

    columns = [
        'company_exchange_id', 'company_industry_id', 'company_sic_id',
        'company_name', 'company_ticket', 'company_is_delisted',
        'company_category', 'company_currency', 'company_location'
    ]

    # Bạn có thể chọn khóa duy nhất để tránh trùng
    conflict_columns = ['company_ticket', 'company_is_delisted']

    insert_data_from_json(latest_file, table_name, columns, conflict_columns)

# load_json_to_db_3()
