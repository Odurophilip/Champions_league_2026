import requests
import csv
import json
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configuration
API_KEY = os.getenv('API_KEY')
BASE_URL = 'https://api.football-data.org/v4'
CHAMPIONS_LEAGUE_CODE = 'CL'  # Champions League competition code

# Global dictionary to store unique teams
teams_dict = {}
team_id_counter = 1

def get_or_create_team_id(team_name, team_api_id=None):
    """Get existing team ID or create new one"""
    global team_id_counter
    if team_name not in teams_dict:
        teams_dict[team_name] = {
            'id': team_id_counter,
            'name': team_name,
            'api_id': team_api_id
        }
        team_id_counter += 1
    return teams_dict[team_name]['id']

def save_teams_table():
    """Save teams reference table"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'teams_{timestamp}.csv'
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['team_id', 'team_name', 'api_id'])
        
        for team_name, team_data in sorted(teams_dict.items(), key=lambda x: x[1]['id']):
            writer.writerow([
                team_data['id'],
                team_data['name'],
                team_data['api_id']
            ])
    
    return filename

def fetch_champions_league_standings():
    """Fetch Champions League league phase standings"""
    
    headers = {
        'X-Auth-Token': API_KEY
    }
    
    url = f'{BASE_URL}/competitions/{CHAMPIONS_LEAGUE_CODE}/standings'
    
    print(f"Fetching standings data from API...")
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if 'standings' in data and len(data['standings']) > 0:
            standings = data['standings'][0]['table']
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f'standings_{timestamp}.csv'
            
            headers_csv = [
                'standing_id',
                'team_id',
                'position',
                'played',
                'won',
                'draw',
                'lost',
                'points',
                'goals_for',
                'goals_against',
                'goal_difference'
            ]
            
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers_csv)
                
                for idx, team in enumerate(standings, 1):
                    team_id = get_or_create_team_id(
                        team['team']['name'],
                        team['team']['id']
                    )
                    
                    writer.writerow([
                        idx,
                        team_id,
                        team['position'],
                        team['playedGames'],
                        team['won'],
                        team['draw'],
                        team['lost'],
                        team['points'],
                        team['goalsFor'],
                        team['goalsAgainst'],
                        team['goalDifference']
                    ])
            
            print(f"✓ Standings: {len(standings)} teams")
            return csv_filename
            
        else:
            print("No standings data found")
            return None
            
    except Exception as e:
        print(f"Error fetching standings: {e}")
        return None

def fetch_champions_league_matches():
    """Fetch Champions League matches and extract goals"""
    
    headers = {
        'X-Auth-Token': API_KEY
    }
    
    url = f'{BASE_URL}/competitions/{CHAMPIONS_LEAGUE_CODE}/matches'
    
    print(f"Fetching matches data from API...")
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if 'matches' in data:
            matches = data['matches']
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            matches_filename = f'matches_{timestamp}.csv'
            goals_filename = f'goals_{timestamp}.csv'
            
            # Matches table
            matches_headers = [
                'match_id',
                'match_day',
                'date',
                'home_team_id',
                'away_team_id',
                'home_score',
                'away_score',
                'status'
            ]
            
            # Goals table
            goals_headers = [
                'goal_id',
                'match_id',
                'team_id',
                'scorer_name',
                'minute',
                'score'
            ]
            
            matches_data = []
            goals_data = []
            goal_id = 1
            
            for match in matches:
                home_team_id = get_or_create_team_id(
                    match['homeTeam']['name'],
                    match['homeTeam']['id']
                )
                away_team_id = get_or_create_team_id(
                    match['awayTeam']['name'],
                    match['awayTeam']['id']
                )
                
                match_id = match['id']
                home_score = match['score']['fullTime']['home']
                away_score = match['score']['fullTime']['away']
                
                matches_data.append([
                    match_id,
                    match.get('matchday', 'N/A'),
                    match['utcDate'],
                    home_team_id,
                    away_team_id,
                    home_score if home_score is not None else '',
                    away_score if away_score is not None else '',
                    match['status']
                ])
                
                # Extract goals if available
                if 'goals' in match and match['goals']:
                    for goal in match['goals']:
                        scorer_team = goal.get('team', {}).get('name', '')
                        scorer_team_id = home_team_id if scorer_team == match['homeTeam']['name'] else away_team_id
                        
                        goals_data.append([
                            goal_id,
                            match_id,
                            scorer_team_id,
                            goal.get('scorer', {}).get('name', 'Unknown') if goal.get('scorer') else 'Unknown',
                            goal.get('minute', ''),
                            goal.get('score', {})
                        ])
                        goal_id += 1
            
            # Save matches
            with open(matches_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(matches_headers)
                writer.writerows(matches_data)
            
            # Save goals
            with open(goals_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(goals_headers)
                writer.writerows(goals_data)
            
            print(f"✓ Matches: {len(matches)} matches")
            print(f"✓ Goals: {len(goals_data)} goals")
            
            return matches_filename, goals_filename
            
    except Exception as e:
        print(f"Error fetching matches: {e}")
        return None, None

def fetch_top_scorers():
    """Fetch Champions League top scorers"""
    
    headers = {
        'X-Auth-Token': API_KEY
    }
    
    url = f'{BASE_URL}/competitions/{CHAMPIONS_LEAGUE_CODE}/scorers'
    
    print(f"Fetching scorers data from API...")
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if 'scorers' in data:
            scorers = data['scorers']
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f'scorers_{timestamp}.csv'
            
            headers_csv = [
                'scorer_id',
                'player_name',
                'team_id',
                'goals',
                'assists',
                'penalties'
            ]
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers_csv)
                
                for idx, scorer in enumerate(scorers, 1):
                    team_id = get_or_create_team_id(
                        scorer['team']['name'],
                        scorer['team']['id']
                    )
                    
                    writer.writerow([
                        idx,
                        scorer['player']['name'],
                        team_id,
                        scorer.get('goals', 0),
                        scorer.get('assists', 0),
                        scorer.get('penalties', 0)
                    ])
            
            print(f"✓ Scorers: {len(scorers)} players")
            return filename
            
    except Exception as e:
        print(f"Error fetching scorers: {e}")
        return None

if __name__ == '__main__':
    print("Champions League Data Fetcher")
    print("=" * 50)
    
    if not API_KEY:
        print("⚠ ERROR: API_KEY not found in .env file!")
        print("Please add your API key to the .env file")
        exit(1)
    
    print("Fetching data and creating relational tables...\n")
    
    # Fetch all data (this populates teams_dict)
    standings_table = fetch_champions_league_standings()
    matches_table, goals_table = fetch_champions_league_matches()
    scorers_table = fetch_top_scorers()
    
    # Save teams reference table
    teams_table = save_teams_table()
    
    print("\n" + "=" * 50)
    print("✓ All tables created with relationships:")
    print(f"     - {teams_table}")
    print(f"     - {matches_table}")
    print(f"     - {goals_table}")
    print(f"     - {standings_table}")
    print(f"     - {scorers_table}")
    print("\nRelationships:")
    print("  - matches.home_team_id → teams.team_id")
    print("  - matches.away_team_id → teams.team_id")
    print("  - goals.team_id → teams.team_id")
    print("  - goals.match_id → matches.match_id")
    print("  - standings.team_id → teams.team_id")
    print("  - scorers.team_id → teams.team_id")
    print("=" * 50)