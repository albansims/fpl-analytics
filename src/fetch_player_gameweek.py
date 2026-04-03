import requests
import json
import os
import time
import sqlite3

DB_PATH = os.path.join('db', 'fpl.db')
SAVE_DIR = os.path.join('data', 'player_gameweek')
BASE_URL = 'https://fantasy.premierleague.com/api/element-summary/{}/'

def get_player_ids():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute('SELECT id FROM players ORDER BY id')
    ids = [row[0] for row in cur.fetchall()]
    con.close()
    return ids

def fetch_player_gameweek():
    os.makedirs(SAVE_DIR, exist_ok=True)

    player_ids = get_player_ids()
    total = len(player_ids)
    print(f'Fetching data for {total} players...')

    for i, player_id in enumerate(player_ids):
        save_path = os.path.join(SAVE_DIR, f'{player_id}.json')

        if os.path.exists(save_path):
            print(f'[{i+1}/{total}] Player {player_id} — already saved, skipping')
            continue

        url = BASE_URL.format(player_id)
        response = requests.get(url)

        if response.status_code != 200:
            print(f'[{i+1}/{total}] Player {player_id} — ERROR {response.status_code}')
            time.sleep(2)
            continue

        data = response.json()

        with open(save_path, 'w') as f:
            json.dump(data, f)

        print(f'[{i+1}/{total}] Player {player_id} — saved ({len(data["history"])} gameweeks)')
        time.sleep(1)

    print('Done.')

if __name__ == '__main__':
    fetch_player_gameweek()