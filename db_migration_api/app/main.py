from fastapi import FastAPI
import pandas as pd
import pyodbc
from dotenv import load_dotenv
import os


# Load the FasAPI 
app = FastAPI()

# Load the dotenv to get some key data for our connection
load_dotenv(dotenv_path="app/.env")

#SQL connections
server_name = os.getenv("server_name")
database_name = os.getenv("database_name")
user_id = os.getenv("user_id")
password = os.getenv("password")
schema = 'dbo'
table_name_deparments= 'deparments'
table_name_hired_employees= 'hired_employees'
table_name_jobs= 'jobs'
driver = '{SQL Server}' #this one is the one using in my local machine
connection_string = "Driver={};Server=tcp:{}.database.windows.net,1433;Database={};Uid={};Pwd={};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;".format(driver,server_name,database_name,user_id,password)

#Conection to SQL
cnx=pyodbc.connect(connection_string)
cursor=cnx.cursor()

#CSV files = columns by request
departments_columns = ["id", "department"]
jobs_columns = ["id", "job"]
hired_employees_columns = ["id", "name", "datetime", "department_id", "job_id"]

# Dataframe creation using Pandas.  
departments_df = pd.read_csv('data/departments.csv', header=None, names=departments_columns)
jobs_df = pd.read_csv('data/jobs.csv', header=None, names=jobs_columns)
hired_employees_df = pd.read_csv('data/hired_employees.csv', header=None, names=hired_employees_columns, na_values=['', 'NULL'],  
    keep_default_na=True)

# We need to check for NULLs in all DF, they are treated differently in Pandas that in SQL
departments_df = departments_df.where(pd.notnull(departments_df),None)
jobs_df = jobs_df.where(pd.notnull(jobs_df),None)
hired_employees_df = hired_employees_df.where(pd.notnull(hired_employees_df),None)

# DF that can contain NULLS needs a special treatment
hired_employees_df['id'] = hired_employees_df['id'].astype('Int64')
hired_employees_df['department_id'] = hired_employees_df['department_id'].astype('Int64')
hired_employees_df['job_id'] = hired_employees_df['job_id'].astype('Int64')

#Need a function to treat the NULLs of this DF
def convert_na_to_none(df):
    # We create a copy not to change the original DF
    df_copy = df.copy()
    
    for col in df_copy.columns:
        # replaces NA wih None
        df_copy[col] = df_copy[col].astype(object).where(~df_copy[col].isna(), None)
    return df_copy

# Apply the function
hired_employees_df = convert_na_to_none(hired_employees_df)

# Create a function to insert the data (batches) into SQL after we Truncate the data of each table
def insert_dataframe_to_sql(cursor, connection, df, table_name, batch_size=1000):
    """
    Inserts a DF to SQL in batches, after TRUNCATE.
    
    Args:
        cursor: Cursor of connection to db
        connection: connection to the db
        df: pandas DataFrame 
        table_name: table name in the db
        batch_size: max batch size
    """
    # Prepare columns and placeholders
    columns = ', '.join(['[{}]'.format(col) for col in df.columns])
    placeholders = ', '.join(['?' for _ in df.columns])
    
    # Truncate the Table
    cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
    connection.commit()
    
    # Query to execute
    query = f"INSERT INTO dbo.{table_name} ({columns}) VALUES ({placeholders})"
    cursor.fast_executemany = True
    
    # Convert DF to a list
    rows = df.values.tolist()
    total_rows = len(rows)
    
    # Insert in batches
    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(query, batch)
        connection.commit()

# Table list and their DF
table_data = [
    {'df': departments_df, 'table_name': 'departments'},
    {'df': hired_employees_df, 'table_name': 'hired_employees'},
    {'df': jobs_df, 'table_name': 'jobs'}
]

# Insert each DF and their data
try:
    for item in table_data:
        insert_dataframe_to_sql(cursor, cnx, item['df'], item['table_name'], batch_size=1000)
except Exception as e:
    print(f"\nError al insertar datos: {e}")
finally:
    cnx.close()

@app.get("/")
def root():
    return {"message": "Task succesfull"}