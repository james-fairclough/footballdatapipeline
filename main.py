from time import sleep
import datetime 
from flask import Flask, request
import os
import threading 
from datetime import datetime
import yaml
import GCPBQUpadates as bqu
import sourceData as sd
import SportsDataAPIRequests as api

season = str(datetime.now().year)

################   YAML Config File Reading   ###################

def get_league_ids(country):
    file_path = os.path.join(app.root_path, "leagues.yaml")
    with open(file_path, "r") as file:
        league_ids = yaml.safe_load(file)

    return league_ids["leagues"][country].values()

def get_all_league_ids():
    file_path = os.path.join(app.root_path, "leagues.yaml")
    with open(file_path, "r") as file:
        league_ids = yaml.safe_load(file)

    league_values = []  # Initialize an empty list

    # Loop through each country in the YAML
    for country, leagues in league_ids["leagues"].items():
        # Loop through each league ID (values)
        for league_id in leagues.values():
            league_values.append(league_id)  # Store league ID
    return league_values


def get_all_countries():
    file_path = os.path.join(app.root_path, "leagues.yaml")
    with open(file_path, "r") as file:
        league_data = yaml.safe_load(file)

    country_names = []  # Initialize an empty list

    for country in league_data["leagues"]:
        country_names.append(country)  # Store country name

    return country_names

def load_league_lookup():
    with open("leagues.yaml", "r") as file:
        league_data = yaml.safe_load(file)

    # Create a dictionary mapping IDs to league names
    league_lookup = {}
    for country, leagues in league_data["leagues"].items():
        for league_name, league_id in leagues.items():
            league_lookup[league_id] = league_name  # Store ID as key, name as value

    return league_lookup

league_lookup_dict = load_league_lookup()

def get_league_name_by_id(league_id):
    return league_lookup_dict.get(league_id, "League ID not found")



###################   Flask App   ###################

# Initialize Flask application
app = Flask(__name__)

# Endpoint to update leagues data
@app.route("/leagues")
def updateLeagues():
    secret = sd.getSecret()
    countries = get_all_countries()  # Default to 'england' if not provided
    for c in countries:    
        leagues = api.getLeagues(secret, c)
        bqu.replaceTable(f'Leagues_{c}', leagues)
    return "Leagues updated successfully!", 200

# Endpoint to update fixtures data
@app.route("/fixtures")
def updateFixtures():
    season = request.args.get('season') 
    secret = sd.getSecret()
    if season == None:
        season = datetime.now().year if datetime.now().month > 6 else datetime.now().year - 1
    fixtures = api.getFixtures(secret,season,39)
    bqu.replaceTable('Fixtures_PL_' + season, fixtures)
    return f"PL {season} season fixtures updated successfully!", 200

# Endpoint to add players data   
def updatePlayers(season, country):
    secret = sd.getSecret()
    leagues = get_league_ids(country)

    for league in leagues:
        print(f"fetching players for league {league} and season {season}")
        players = api.getPlayers(secret,season,league)
        bqu.replaceTable(f'Players_info_{country}_{get_league_name_by_id(league)}_{season}', players)

    print(f"PL {season} season players updated successfully!")

@app.route("/players")
def updatePlayersRoute():
    season = request.args.get('season') 
    country = request.args.get('country', 'england')  # Default to 'england' if not provided
    if season == None:
        season = datetime.now().year if datetime.now().month > 6 else datetime.now().year - 1

    updatePlayers(season, country)
    return f"{country}, {season} season players' stats updated successfully!", 200

# Endpoint to add players data   
def updatePlayersStats(season, country):
    secret = sd.getSecret() # Fetch secret API key
    leagues = get_league_ids(country) # Fetch league IDs from YAML config

    for league in leagues:
        print(f"fetching players stats for league {league} and season {season}")
        players = api.getPlayerStatistics(secret,season,league)
        bqu.replaceTable(f'Players_stats_{country}_{get_league_name_by_id(league)}_{season}', players)

    print(f"{country}, {season} season players' stats updated successfully!")

@app.route("/playerstats")
def updatePlayersStatsRoute():
    season = request.args.get('season') 
    country = request.args.get('country','england') 

    if season == None:
        season = datetime.now().year if datetime.now().month > 6 else datetime.now().year - 1

    updatePlayersStats(season, country)

    return f"{country}, {season} season players' stats updated successfully!", 200


#Update teams data

def updateTeams(country):
    secret = sd.getSecret()
    teams = api.getTeams(secret, country)
    bqu.replaceTable(f'teams_{country}', teams)
    print(f"{country} teams' updated successfully!")

@app.route("/teams")
def updateTeamsRoute():
    countries = get_all_countries()  # Default to 'england' if not provided
    for c in countries:    
        updateTeams(c)

    return "Teams updated successfully!", 200

# Endpoint to add match event data   
@app.route("/match_events")
def updateMatchEvents():
    season = request.args.get('season') 
    secret = sd.getSecret()
    if season == "":
        season = str(datetime.now().year)
    match_events = api.getMatchEvents(secret,season,39) 
    bqu.replaceTable('Match_Events_PL_' + season, match_events)
    return f"PL {season} season match events updated successfully!", 200

# Endpoint to update all data in parallel
@app.route("/all")
def updateAll():

    season = request.args.get('season') 

    countries = get_all_countries()  # Fetch all league IDs from YAML config


    if season == None:
        season = datetime.now().year if datetime.now().month > 6 else datetime.now().year - 1

    for c in countries:
    #for c in ['england', 'germany']:
        print(f"Updating data for {c} in season {season}...")
        # Create threads for updating data
        #thread1 = threading.Thread(target=updateTeams, args=(c, ))
        #thread2 = threading.Thread(target=updatePlayers, args=(season, c))
        thread3 = threading.Thread(target=updatePlayersStats, args=(season, c))

        # Start threads
        #thread1.start()
        #thread2.start()
        thread3.start()


        # Wait for threads to complete
        #thread1.join()
        #thread2.join()
        thread3.join()
        #sleep(60)
    return "All tables updated successfully!", 200




# Uncomment the following to run the Flask app locally
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

