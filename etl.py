# Import the necessary libraries
import pyodbc
import psycopg2
from psycopg2.extensions import adapt

def load_data_from_sql_server_to_postgres(sql_server_conn_string, postgres_conn_string, source_table, target_table, columns_to_select):
    try:
        # Connect to SQL Server
        sql_server_conn = pyodbc.connect(sql_server_conn_string)
        sql_server_cursor = sql_server_conn.cursor()

        # Connect to PostgreSQL
        postgres_conn = psycopg2.connect(postgres_conn_string)
        postgres_cursor = postgres_conn.cursor()

        # Fetch data from SQL Server for specified columns
        sql_server_query = f"SELECT {', '.join(columns_to_select)} FROM {source_table}"
        sql_server_cursor.execute(sql_server_query)
        rows = sql_server_cursor.fetchall()

        # Prepare the PostgreSQL INSERT statement for specified columns
        insert_query = f"INSERT INTO {target_table} ({', '.join(columns_to_select)}) VALUES ({', '.join(['%s' for _ in columns_to_select])})"

        # Insert the data into PostgreSQL, adapting the values and handling None values
        for row in rows:
            adapted_row = [adapt(value) if value is not None else None for value in row]
            postgres_cursor.execute(insert_query, adapted_row)
        
        postgres_conn.commit()

        # Close connections
        sql_server_cursor.close()
        sql_server_conn.close()
        postgres_cursor.close()
        postgres_conn.close()

        print("Data loaded successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")

# Running The Code
sql_server_conn_string = "DRIVER={SQL Server Native Client 11.0};SERVER=LAPTOP-S05RS544\\SQLEXPRESS;DATABASE=AdventureWorks2012;Trusted_Connection=yes;"
postgres_conn_string = "dbname=staging2 user=postgres password= host=localhost"
source_table = ""
target_table = ""
columns_to_select = [""]  # Add the columns you want to select

load_data_from_sql_server_to_postgres(sql_server_conn_string, postgres_conn_string, source_table, target_table, columns_to_select)
