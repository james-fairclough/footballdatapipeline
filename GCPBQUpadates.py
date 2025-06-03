import pandas as pd
import pandas_gbq 
from google.cloud import bigquery as bq
from google.cloud import secretmanager as sm
from time import sleep
import datetime 
from datetime import datetime, timezone


###################   GCP BigQuery Table Updates   ###################

# Replaces a BigQuery table with new data
def replaceTable(table, df):
    t = 'PlayerComparisons.' + table  # Define table name
    df = clean_dataframe(df)
    pandas_gbq.to_gbq(df, t, project_id='footballdataproject-458811', if_exists='replace')  # Replace table
    createLog(table, "Replace")  # Log the action

# Appends new data to an existing BigQuery table
def updateTable(table, df):
    t = 'APIFootballSource.' + table
    pandas_gbq.to_gbq(df, t, project_id='footballdataproject-458811', if_exists='append')
    createLog(table, "Update")

# Creates a new BigQuery table if it doesn't exist
def createTable(table, df, schema):
    t = 'APIFootballSource.' + table
    pandas_gbq.to_gbq(df, t, project_id='footballdataproject-458811', if_exists='fail', table_schema=schema)
    createLog(table, "Create")

# Logs table actions to a logging table in BigQuery
def createLog(t, action):
    current_utc_time = datetime.now(timezone.utc)  # Get current UTC time
    data = {"table": t, "utctimestamp": current_utc_time, "action": action}  # Create log data
    df = pd.DataFrame([data])  # Convert to DataFrame
    pandas_gbq.to_gbq(df, "APIFootballSource.Logs", project_id='footballdataproject-458811', if_exists='append')  # Append log to BigQuery

# Clean DF
def clean_dataframe(df):
    # Ensure all int64 columns are numeric, filling invalid values with 0
    int_columns = df.select_dtypes(include=['int64']).columns
    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
        print(str(col) + " int")  

    # Ensure datetime columns are parsed correctly
    datetime_columns = df.select_dtypes(include=['datetime64[ns]']).columns
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        print(str(col) + " date")

    # Ensure string columns are consistent and replace NaN with empty strings
    str_columns = df.select_dtypes(include=['string']).columns
    for col in str_columns:
        df[col] = df[col].fillna('').astype('str')
        print(str(col) + " str")

    for col, dtype in df.dtypes.items():
        print(f"Column: {col}, Data Type: {dtype}")
    df = df.fillna('').astype('str')
    print(df)
    return df