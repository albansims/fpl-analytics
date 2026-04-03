import sqlite3
import json
import os

DB_PATH = os.path.join('db', 'fpl.db')
SAVE_DIR = os.path.join('data', 'player_gameweek')

def load_player_gameweek():
    con = sqlite3.connect(DB_PATH)
    con.execute('PRAGMA foreign_keys = ON')
    cur = con.cursor()

    files = os.listdir(SAVE_DIR)
    total = len(files)
    total_rows = 0

    for i, filename in enumerate(files):
        player_id = int(filename.replace('.json', ''))
        filepath = os.path.join(SAVE_DIR, filename)

        with open(filepath, 'r') as f:
            data = json.load(f)

        rows = []
        for gw in data['history']:
            rows.append((
                player_id,
                gw['round'],
                gw['opponent_team'],
                1 if gw['was_home'] else 0,
                gw['value'],
                gw['total_points'],
                gw['minutes'],
                gw['goals_scored'],
                gw['assists'],
                gw['clean_sheets'],
                gw['goals_conceded'],
                gw['own_goals'],
                gw['penalties_saved'],
                gw['penalties_missed'],
                gw['yellow_cards'],
                gw['red_cards'],
                gw['saves'],
                gw['bonus'],
                gw['bps'],
                gw['starts'],
                float(gw['expected_goals']),
                float(gw['expected_assists']),
                float(gw['expected_goal_involvements']),
                float(gw['expected_goals_conceded']),
                float(gw['influence']),
                float(gw['creativity']),
                float(gw['threat']),
                float(gw['ict_index']),
                gw['selected'],
                gw['transfers_in'],
                gw['transfers_out'],
            ))

        cur.executemany("""
            INSERT OR REPLACE INTO player_gameweek_stats (
                player_id, gameweek_id, opponent_team_id, was_home, value,
                total_points, minutes, goals_scored, assists, clean_sheets,
                goals_conceded, own_goals, penalties_saved, penalties_missed,
                yellow_cards, red_cards, saves, bonus, bps, starts,
                expected_goals, expected_assists, expected_goal_involvements,
                expected_goals_conceded, influence, creativity, threat, ict_index,
                selected, transfers_in, transfers_out
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?
            )
        """, rows)

        total_rows += len(rows)

        if (i + 1) % 100 == 0 or (i + 1) == total:
            print(f'[{i+1}/{total}] Processed — {total_rows} rows so far')

    con.commit()
    con.close()
    print(f'Done. {total_rows} rows loaded into player_gameweek_stats.')

if __name__ == '__main__':
    load_player_gameweek()