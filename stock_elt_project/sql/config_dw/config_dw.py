import duckdb
import os

# DuckDB file path
database_path = '/home/thangtranquoc/projects/stock_elt_project/datawarehouse.duckdb'

# Remove database file if already exists
if os.path.exists(database_path):
    os.remove(database_path)

# Connect to DuckDB, open or create database
conn = duckdb.connect(database=database_path)

# Read the query of the sql file
with open('/home/thangtranquoc/projects/stock_elt_project/sql/config_dw/datawarehouse.sql','r') as file:
    query = file.read()

# Execute the query
conn.execute(query)

# Close the connection
conn.close()

print(f'Database has been created and saved to {database_path}')