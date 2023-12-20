# Import the necessary libraries
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

# Get the necessary environment variablepwd = os.environ.get('PGPASS')
uid = os.environ.get('PGUID')

# SQL Server database details
# Use the "SQL Server native client 11.0"
#driver = "{SQL Server Native Client 11.0}"
#server = "CALVIN"
#database = "AdventureWorksDW2019;"

# Initialize rows_imported and src_conn
rows_imported = 0
src_conn = None

# Extract data from SQL Server
def extract():
    global rows_imported
    global src_conn

    try:
        # Use ODBC Driver 17 for SQL Server
        # src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '/SQLEXPRESS, 1434' + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)
        #src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '/SQLEXPRESS' + ';DATABASE=' + database)
        src_conn_str = ("Driver={SQL Server Native Client 11.0};"
            "Server=LAPTOP-S05RS544\\SQLEXPRESS;"
            "Database=AdventureWorks2012;"
            "Trusted_Connection=yes;")
        src_conn = pyodbc.connect(src_conn_str)
        src_cursor = src_conn.cursor()
        # Execute query
        src_cursor.execute(""" select  t.name as table_name
        from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """)
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            # Query and load data into a dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        if src_conn:
            src_conn.close()

# Load data into PostgreSQL
def load(df, tbl):
    global rows_imported

    try:
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/AdventureWorks')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # Save df to PostgreSQL
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # Add elapsed time to the final printout
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    # Call the extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))