import requests 
import json 
import pandas as pd
import pandas_gbq 
from google.cloud import bigquery as bq
from google.cloud import secretmanager as sm
from time import sleep
import datetime 
from flask import Flask, request
import os
import threading 
from datetime import datetime, timezone
import pyarrow as pa

season = str(datetime.now().year)

###################   API Requests   ###################

# Retrieves the list of datasets from the BigQuery project
def getDatasets():
    client = bq.Client(project='footballdataproject-458811')  # Instantiate BigQuery client
    datasets = list(client.list_datasets())  # List all datasets in the project
    project = client.project
    print("Datasets in project {}:".format(project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))  # Print each dataset ID

# Retrieves the secret API key stored in Google Secret Manager
def getSecret():
    client = sm.SecretManagerServiceClient()
    name = f"projects/footballdataproject-458811/secrets/Sport-Data-Keys/versions/1"  # Secret resource name
    response = client.access_secret_version(request={"name": name})  # Access the secret
    return response.payload.data.decode("UTF-8")  # Decode and return the secret value

# Fetches team data from the AFL API and returns it as a DataFrame
def getTeams(secret):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    url = "https://v3.football.api-sports.io/teams?country=england"
    payload = {}
    r = make_request_with_retries(url, headers, payload)  # API request with retry logic
    r = json.loads(r.text)['response']  # Parse response
    r = pd.json_normalize(r, sep='_')
    return r  # Return as DataFrame

def getLeagues(secret):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    url = "https://v3.football.api-sports.io/leagues?country=England"
    payload = {}
    r = make_request_with_retries(url, headers, payload)  # API request with retry logic
    r = json.loads(r.text)['response']  # Parse response
    r = pd.json_normalize(r, sep='_')
    return r  # Return as DataFrame

def getFixtures(secret,season,league):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    url = f"https://v3.football.api-sports.io/fixtures?league={league}&season={season}"
    payload = {}
    r = make_request_with_retries(url, headers, payload)  # API request with retry logic
    r = json.loads(r.text)['response']  # Parse response
    r = pd.json_normalize(r, sep='_')
    return r  # Return as DataFrame

def getPlayers(secret,season,league, page = 1, player_data = pd.DataFrame()):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    url = f"https://v3.football.api-sports.io/players?league={league}&season={season}&page={page}"
    payload = {}
    r = make_request_with_retries(url, headers, payload)  # API request with retry logic
    p = json.loads(r.text)['paging']
    r = json.loads(r.text)['response']
    r = pd.json_normalize(r, sep='_')
    r = r.drop('statistics', axis=1)
    
    player_data = pd.concat([player_data, r], ignore_index=True)

    if p["current"] < p["total"]:
        page = p["current"] + 1

        if page == 4:
            return player_data # added as current sub doesnt allow access past page 3
        
        player_data = getPlayers(secret,season,league, page, player_data)

    return player_data  

def getMatchEvents(secret,season,league):
    
    fixtures = getFixtures(secret,season,league)["fixture_id"]
    all_match_events = pd.DataFrame()

    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    payload = {}

    request_counter = 0 

    for f in fixtures:
        request_counter += 1
        url = f"https://v3.football.api-sports.io/fixtures/events?fixture={f}"
        r = make_request_with_retries(url, headers, payload)  # API request with retry logic
        events = json.loads(r.text)['response']  # Parse response
        for event in events:
            event['fixture_id'] = f  # add fixture ID to each event
        events = pd.json_normalize(events, sep='_')
        all_match_events = pd.concat([all_match_events, events], ignore_index=True)

        if request_counter == 10: 
            break # added so that dont exceed sub limits

    return all_match_events  # Return as DataFrame

# Helper function to handle API requests with retries
def make_request_with_retries(url, headers, payload):
    retries = 0
    while retries < 3:  # Retry up to 3 times
        try:
            r = requests.request("GET", url, headers=headers, data=payload)  # Make API request
            if r and r.text:  # Check for a valid response
                return r
            else:
                print(f"Attempt {retries + 1}: Empty response. Retrying...")
        except requests.RequestException as e:
            print(f"Attempt {retries + 1}: Error occurred: {e}. Retrying...")
        retries += 1
        sleep(1)  # Wait before retrying

###################   GCP BigQuery Table Updates   ###################

# Replaces a BigQuery table with new data
def replaceTable(table, df):
    t = 'APIFootballSource.' + table  # Define table name
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

###################   Flask App   ###################

# Initialize Flask application
app = Flask(__name__)

# Endpoint to update leagues data
@app.route("/leagues")
def updateLeagues():
    secret = getSecret()
    leagues = getLeagues(secret)
    replaceTable('Leagues', leagues)
    return "Leagues updated successfully!", 200

# Endpoint to update fixtures data
@app.route("/plfixtures")
def updateFixtures():
    season = request.args.get('season') 
    secret = getSecret()
    if season == "":
        season = str(datetime.now().year)
    fixtures = getFixtures(secret,season,39)
    replaceTable('Fixtures_PL_' + season, fixtures)
    return f"PL {season} season fixtures updated successfully!", 200

# Endpoint to add players data   
@app.route("/plplayers")
def updatePlayers():
    season = request.args.get('season') 
    secret = getSecret()
    if season == "":
        season = str(datetime.now().year)
    players = getPlayers(secret,season,39)
    replaceTable('Players_PL_' + season, players)
    return f"PL {season} season players updated successfully!", 200

# Endpoint to update all data in parallel
@app.route("/all")
def updateAll():
    # Create threads for updating data
    thread1 = threading.Thread(target=updateLeagues)
    thread2 = threading.Thread(target=updateTeams)


    # Start threads
    thread1.start()
    thread2.start()


    # Wait for threads to complete
    thread1.join()
    thread2.join()

    return "All tables updated successfully!", 200

# Similar endpoints for other update functions
@app.route("/teams")
def updateTeams():
    secret = getSecret()
    teams = getTeams(secret)
    replaceTable('Teams', teams)
    return "Teams updated successfully!", 200

# Endpoint to add match event data   
@app.route("/plevents")
def updateMatchEvents():
    season = request.args.get('season') 
    secret = getSecret()
    if season == "":
        season = str(datetime.now().year)
    match_events = getMatchEvents(secret,season,39) 
    replaceTable('Match_Events_PL_' + season, match_events)
    return f"PL {season} season match events updated successfully!", 200

# Uncomment the following to run the Flask app locally
if __name__ == "__main__":
     app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))