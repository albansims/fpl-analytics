"""
Load historical gameweek stats from vaastav CSV files into player_gameweek_stats.

Input:  data/historical/{season}/gws/GW{n}.csv — vaastav archive files
        db/fpl.db — reads valid player IDs to skip players not in current season
Output: Populates player_gameweek_stats in db/fpl.db for seasons 2022-23 to 2024-25

Foreign key enforcement is disabled during load — historical fixtures include
teams no longer in the Premier League (e.g. Leicester, Ipswich) which do
not exist in the teams table. 

Safe to re-run — uses INSERT OR REPLACE so existing rows are overwritten.
Run after load_player_gameweek.py.
"""

import sqlite3
import pandas as pd
import os

DB_PATH = os.path.join('db', 'fpl.db')

SEASONS = ['2022-23', '2023-24', '2024-25']

# Vaastav team name → your teams table name
# Only needed for opponent mapping — player matching uses element ID directly
TEAM_NAME_MAP = {
    'Arsenal': 'Arsenal',
    'Aston Villa': 'Aston Villa',
    'Bournemouth': 'Bournemouth',
    'Brentford': 'Brentford',
    'Brighton': 'Brighton',
    'Burnley': 'Burnley',
    'Chelsea': 'Chelsea',
    'Crystal Palace': 'Crystal Palace',
    'Everton': 'Everton',
    'Fulham': 'Fulham',
    'Leeds': 'Leeds',
    'Leicester': 'Leicester',
    'Liverpool': 'Liverpool',
    'Man City': 'Man City',
    'Man Utd': 'Man Utd',
    'Newcastle': 'Newcastle',
    "Nott'm Forest": "Nott'm Forest",
    'Sheffield Utd': 'Sheffield Utd',
    'Southampton': 'Southampton',
    'Spurs': 'Spurs',
    'West Ham': 'West Ham',
    'Wolves': 'Wolves',
    'Ipswich': 'Ipswich',
    'Luton': 'Luton',
    'Nottingham Forest': "Nott'm Forest",
    'Tottenham': 'Spurs',
    'Manchester City': 'Man City',
    'Manchester Utd': 'Man Utd',
}


def load_historical():
    con = sqlite3.connect(DB_PATH)
    con.execute('PRAGMA foreign_keys = OFF')  # OFF because historical teams may not be in teams table
    cur = con.cursor()

    # Get all valid player IDs from the database
    cur.execute("SELECT id FROM players")
    valid_player_ids = {row[0] for row in cur.fetchall()}

    total_rows = 0
    skipped_players = 0
    skipped_rows = 0

    for season in SEASONS:
        season_rows = 0
        gw_dir = os.path.join('data', 'historical', season, 'gws')

        if not os.path.exists(gw_dir):
            print(f"Directory not found: {gw_dir} — skipping")
            continue

        files = sorted(os.listdir(gw_dir))
        print(f"\nLoading {season} — {len(files)} gameweek files found")

        for filename in files:
            if not filename.endswith('.csv'):
                continue

            filepath = os.path.join(gw_dir, filename)
            df = pd.read_csv(filepath, encoding='utf-8')

            rows = []
            for _, row in df.iterrows():
                player_id = int(row['element'])

                # Skip players not in our players table
                if player_id not in valid_player_ids:
                    skipped_players += 1
                    continue

                opponent = TEAM_NAME_MAP.get(str(row['opponent_team']).strip(), str(row['opponent_team']).strip())

                # Handle expected stats — older seasons may have nulls
                def safe_float(val):
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return None

                rows.append((
                    player_id,
                    int(row['round']),
                    season,
                    opponent,
                    1 if row['was_home'] else 0,
                    int(row['value']) if pd.notna(row['value']) else None,
                    int(row['total_points']),
                    int(row['minutes']),
                    int(row['goals_scored']),
                    int(row['assists']),
                    int(row['clean_sheets']),
                    int(row['goals_conceded']),
                    int(row['own_goals']),
                    int(row['penalties_saved']),
                    int(row['penalties_missed']),
                    int(row['yellow_cards']),
                    int(row['red_cards']),
                    int(row['saves']),
                    int(row['bonus']),
                    int(row['bps']),
                    int(row['starts']) if pd.notna(row.get('starts', None)) else None,
                    safe_float(row['expected_goals']),
                    safe_float(row['expected_assists']),
                    safe_float(row['expected_goal_involvements']),
                    safe_float(row['expected_goals_conceded']),
                    safe_float(row['influence']),
                    safe_float(row['creativity']),
                    safe_float(row['threat']),
                    safe_float(row['ict_index']),
                    int(row['selected']) if pd.notna(row['selected']) else None,
                    int(row['transfers_in']) if pd.notna(row['transfers_in']) else None,
                    int(row['transfers_out']) if pd.notna(row['transfers_out']) else None,
                ))

            cur.executemany("""
                INSERT OR REPLACE INTO player_gameweek_stats (
                    player_id, gameweek_id, season, opponent_team, was_home, value,
                    total_points, minutes, goals_scored, assists, clean_sheets,
                    goals_conceded, own_goals, penalties_saved, penalties_missed,
                    yellow_cards, red_cards, saves, bonus, bps, starts,
                    expected_goals, expected_assists, expected_goal_involvements,
                    expected_goals_conceded, influence, creativity, threat, ict_index,
                    selected, transfers_in, transfers_out
                ) VALUES (
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?
                )
            """, rows)

            season_rows += len(rows)
            skipped_rows += skipped_players

        print(f"{season} — {season_rows} rows loaded")
        total_rows += season_rows

    con.commit()
    con.close()
    print(f"\nDone. {total_rows} historical rows loaded.")
    print(f"Skipped {skipped_rows} rows where player ID not in players table.")


if __name__ == '__main__':
    load_historical()