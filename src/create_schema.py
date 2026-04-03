import sqlite3
import os

DB_PATH = os.path.join('db', 'fpl.db')

def create_schema():
    os.makedirs('db', exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.executescript("""
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS positions (
            id                  INTEGER PRIMARY KEY,
            singular_name       TEXT NOT NULL,
            singular_name_short TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS teams (
            id                    INTEGER PRIMARY KEY,
            name                  TEXT NOT NULL,
            short_name            TEXT NOT NULL,
            strength              INTEGER,
            strength_overall_home INTEGER,
            strength_overall_away INTEGER,
            strength_attack_home  INTEGER,
            strength_attack_away  INTEGER,
            strength_defence_home INTEGER,
            strength_defence_away INTEGER
        );

        CREATE TABLE IF NOT EXISTS players (
            id                          INTEGER PRIMARY KEY,
            first_name                  TEXT NOT NULL,
            second_name                 TEXT NOT NULL,
            web_name                    TEXT NOT NULL,
            team_id                     INTEGER NOT NULL REFERENCES teams(id),
            position_id                 INTEGER NOT NULL REFERENCES positions(id),
            now_cost                    INTEGER,
            selected_by_percent         REAL,
            transfers_in                INTEGER,
            transfers_out               INTEGER,
            total_points                INTEGER,
            minutes                     INTEGER,
            goals_scored                INTEGER,
            assists                     INTEGER,
            clean_sheets                INTEGER,
            goals_conceded              INTEGER,
            saves                       INTEGER,
            bonus                       INTEGER,
            bps                         INTEGER,
            yellow_cards                INTEGER,
            red_cards                   INTEGER,
            starts                      INTEGER,
            expected_goals              REAL,
            expected_assists            REAL,
            expected_goal_involvements  REAL,
            expected_goals_conceded     REAL,
            influence                   REAL,
            creativity                  REAL,
            threat                      REAL,
            ict_index                   REAL,
            form                        REAL,
            points_per_game             REAL,
            ep_next                     REAL
        );

        CREATE TABLE IF NOT EXISTS gameweeks (
            id                  INTEGER PRIMARY KEY,
            name                TEXT NOT NULL,
            deadline_time       TEXT,
            average_entry_score INTEGER,
            highest_score       INTEGER,
            finished            INTEGER
        );

        CREATE TABLE IF NOT EXISTS player_gameweek_stats (
            player_id                   INTEGER NOT NULL REFERENCES players(id),
            gameweek_id                 INTEGER NOT NULL REFERENCES gameweeks(id),
            season                      TEXT NOT NULL,
            opponent_team               TEXT,
            was_home                    INTEGER,
            value                       INTEGER,
            total_points                INTEGER,
            minutes                     INTEGER,
            goals_scored                INTEGER,
            assists                     INTEGER,
            clean_sheets                INTEGER,
            goals_conceded              INTEGER,
            own_goals                   INTEGER,
            penalties_saved             INTEGER,
            penalties_missed            INTEGER,
            yellow_cards                INTEGER,
            red_cards                   INTEGER,
            saves                       INTEGER,
            bonus                       INTEGER,
            bps                         INTEGER,
            starts                      INTEGER,
            expected_goals              REAL,
            expected_assists            REAL,
            expected_goal_involvements  REAL,
            expected_goals_conceded     REAL,
            influence                   REAL,
            creativity                  REAL,
            threat                      REAL,
            ict_index                   REAL,
            selected                    INTEGER,
            transfers_in                INTEGER,
            transfers_out               INTEGER,
            PRIMARY KEY (player_id, gameweek_id, season)
        );
    """)

    con.commit()
    con.close()
    print(f'Database created at {DB_PATH}')

if __name__ == '__main__':
    create_schema()
