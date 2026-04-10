"""
Load positions, teams, players, and gameweeks from the saved bootstrap JSON.

Input:  data/bootstrap_raw.json - already produced by fetch_bootstrap.py
Output: Populates the four tables in database - db/fpl.db: positions, teams, players, gameweeks

Must be run after create_schema.py and after fetch_bootstrap.py.
Safe to re-run — uses INSERT OR REPLACE so existing rows are overwritten.
"""


import sqlite3
import json
import os

DB_PATH = os.path.join("db", "fpl.db")
DATA_PATH = os.path.join("data", "bootstrap_raw.json")

def load_bootstrap():
    with open(DATA_PATH, "r") as f:
        data = json.load(f)

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON")
    cur = con.cursor()

    # --- positions ---
    for p in data["element_types"]:
        cur.execute("""
            INSERT OR REPLACE INTO positions (id, singular_name, singular_name_short)
            VALUES (?, ?, ?)
        """, (
            p["id"],
            p["singular_name"],
            p["singular_name_short"]
        ))
    print(f"Loaded {len(data['element_types'])} positions")

    # --- teams ---
    for t in data["teams"]:
        cur.execute("""
            INSERT OR REPLACE INTO teams (
                id, name, short_name, strength,
                strength_overall_home, strength_overall_away,
                strength_attack_home, strength_attack_away,
                strength_defence_home, strength_defence_away
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            t["id"],
            t["name"],
            t["short_name"],
            t["strength"],
            t["strength_overall_home"],
            t["strength_overall_away"],
            t["strength_attack_home"],
            t["strength_attack_away"],
            t["strength_defence_home"],
            t["strength_defence_away"]
        ))
    print(f"Loaded {len(data['teams'])} teams")

    # --- players ---
    for pl in data["elements"]:
        cur.execute("""
            INSERT OR REPLACE INTO players (
                id, first_name, second_name, web_name,
                team_id, position_id,
                now_cost, selected_by_percent,
                transfers_in, transfers_out,
                total_points, minutes, goals_scored, assists,
                clean_sheets, goals_conceded, saves,
                bonus, bps, yellow_cards, red_cards, starts,
                expected_goals, expected_assists,
                expected_goal_involvements, expected_goals_conceded,
                influence, creativity, threat, ict_index,
                form, points_per_game, ep_next
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pl["id"],
            pl["first_name"],
            pl["second_name"],
            pl["web_name"],
            pl["team"],
            pl["element_type"],
            pl["now_cost"],
            float(pl["selected_by_percent"]),
            pl["transfers_in"],
            pl["transfers_out"],
            pl["total_points"],
            pl["minutes"],
            pl["goals_scored"],
            pl["assists"],
            pl["clean_sheets"],
            pl["goals_conceded"],
            pl["saves"],
            pl["bonus"],
            pl["bps"],
            pl["yellow_cards"],
            pl["red_cards"],
            pl["starts"],
            float(pl["expected_goals"]),
            float(pl["expected_assists"]),
            float(pl["expected_goal_involvements"]),
            float(pl["expected_goals_conceded"]),
            float(pl["influence"]),
            float(pl["creativity"]),
            float(pl["threat"]),
            float(pl["ict_index"]),
            float(pl["form"]),
            float(pl["points_per_game"]),
            float(pl["ep_next"]) if pl["ep_next"] is not None else None
        ))
    print(f"Loaded {len(data['elements'])} players")

    # --- gameweeks ---
    for gw in data["events"]:
        cur.execute("""
            INSERT OR REPLACE INTO gameweeks (
                id, name, deadline_time,
                average_entry_score, highest_score,
                finished
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            gw["id"],
            gw["name"],
            gw["deadline_time"],
            gw["average_entry_score"],
            gw["highest_score"],
            1 if gw["finished"] else 0
        ))
    print(f"Loaded {len(data['events'])} gameweeks")


    con.commit()
    con.close()
    print("Done.")

if __name__ == "__main__":
    load_bootstrap()