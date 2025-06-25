import duckdb

# Connect to DuckDB
conn = duckdb.connect('/home/thangtranquoc/projects/stock_elt_project/datawarehouse.duckdb')

# Delete fact_candles table if exists
# conn.execute('DROP TABLE IF EXISTS fact_candles CASCADE;')

# Create new fact_candles table
create_table_query = """
    CREATE TABLE IF NOT EXISTS fact_candles(
        candle_id INTEGER DEFAULT NEXTVAL('candle_id_seq') PRIMARY KEY,
        candle_company_id INTEGER NOT NULL,
        candle_volume INTEGER NOT NULL,
        candle_volumne_weighted DOUBLE NOT NULL,
        candle_open DOUBLE NOT NULL,
        candle_close DOUBLE NOT NULL,
        candle_high DOUBLE NOT NULL,
        candle_low DOUBLE NOT NULL,
        candle_time_stamp CHAR(15) NOT NULL,
        candle_is_otc BOOLEAN DEFAULT false,
        candles_time_id INTEGER,
        FOREIGN KEY(candle_company_id) REFERENCES dim_companies(company_id),
        FOREIGN KEY(candles_time_id) REFERENCES dim_time(time_id)
    );
"""

conn.execute(create_table_query)

# Close the connection
conn.close()