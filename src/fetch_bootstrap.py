"""
Fetch current season data from the FPL bootstrap-static endpoint.

Input:  None needed. This calls the FPL API directly (no authentication required)
Output: data/bootstrap_raw.json. THis is the raw API response which contains all players,
        teams, positions, and gameweeks for the current season
"""

import requests 
import json

url = "https://fantasy.premierleague.com/api/bootstrap-static/"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    output_path = "data/bootstrap_raw.json"
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Data successfully saved to {output_path}")
    
    print(f"Keys in the JSON data: {list(data.keys())}")

else: 
    print(f"Failed to retrieve data. Status code: {response.status_code}")
