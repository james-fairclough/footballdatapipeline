import requests 
import json 
import pandas as pd
from google.cloud import bigquery as bq
from google.cloud import secretmanager as sm
from time import sleep


###################   API Requests   ###################



# Fetches team data from the AFL API and returns it as a DataFrame
def getTeams(secret, country = 'england'):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    url = f"https://v3.football.api-sports.io/teams?country={country}"
    payload = {}
    r = make_request_with_retries(url, headers, payload)  # API request with retry logic
    r = json.loads(r.text)['response']  # Parse response
    r = pd.json_normalize(r, sep='_')
    return r  # Return as DataFrame

def getLeagues(secret, country = 'england'):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    url = f"https://v3.football.api-sports.io/leagues?country={country}"
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
    print(f"Errors: {json.loads(r.text)['errors']}")
    r = json.loads(r.text)['response']
    r = pd.json_normalize(r, sep='_')
    r = r.drop('statistics', axis=1)

    player_data = pd.concat([player_data, r], ignore_index=True)

    if p["current"] < p["total"]:
        page = p["current"] + 1

        #if page == 4:
        #    return player_data # added as current sub doesnt allow access past page 3
        sleep(.25)
        player_data = getPlayers(secret,season,league, page, player_data)

    return player_data  

def getPlayerStatistics(secret,season,league, page = 1, player_stats = pd.DataFrame()):
    headers = {
        'x-rapidapi-key': secret,
        'x-rapidapi-host': 'v3.football.api-sports.io'
    }
    url = f"https://v3.football.api-sports.io/players?league={league}&season={season}&page={page}"
    payload = {}
    r = make_request_with_retries(url, headers, payload)  # API request with retry logic
    p = json.loads(r.text)['paging']
    print(f"Errors: {json.loads(r.text)['errors']}")
    r = json.loads(r.text)['response']

    stats_list = []  # Initialize an empty list to store player statistics

    for player in r:
        player_id = player["player"]["id"]  # Extract player ID
        for stat in player["statistics"]:  # Loop through their statistics
            stat["player_id"] = player_id  # Associate player ID
            stats_list.append(stat)
        

    # Normalize the statistics data into a DataFrame
    stats_df = pd.json_normalize(stats_list, sep='_')

    
    player_stats = pd.concat([player_stats, stats_df], ignore_index=True)

    if p["current"] < p["total"]:
        page = p["current"] + 1

        #if page == 4:
        #    return player_stats # added as current sub doesnt allow access past page 3
        sleep(.25)
        player_stats = getPlayerStatistics(secret,season,league, page, player_stats)

    return player_stats 

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

