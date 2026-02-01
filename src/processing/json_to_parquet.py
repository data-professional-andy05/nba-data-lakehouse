import os
import json
import gzip
import pandas as pd
from tqdm import tqdm
from src.utils import common

# --- 1. BOXSCORE STATS PROCESSOR ---
def process_boxscore(season):
    """
    Reads Bronze Boxscore (V3) and creates:
    1. fact_player_game_stats.parquet
    2. fact_team_game_stats.parquet
    """
    bronze_dir = os.path.join(common.get_data_dir('bronze'), 'boxscore', f"season={season}")
    silver_base = common.get_data_dir('silver')
    
    player_out = os.path.join(silver_base, 'fact_player_game_stats')
    team_out = os.path.join(silver_base, 'fact_team_game_stats')
    os.makedirs(player_out, exist_ok=True)
    os.makedirs(team_out, exist_ok=True)

    if not os.path.exists(bronze_dir):
        print(f"Skipping {season} (No Bronze Data)")
        return

    # Sort files to process in order (useful for debugging)
    files = sorted([f for f in os.listdir(bronze_dir) if f.endswith('.json.gz')])
    print(f"Processing {len(files)} boxscores for {season}...")

    all_player_stats = []
    all_team_stats = []

    for filename in tqdm(files, desc="Parsing Boxscores"):
        game_id = filename.split('.')[0]
        filepath = os.path.join(bronze_dir, filename)
        
        try:
            with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                data = json.load(f)
            
            # Root -> boxScoreTraditional
            box = data.get('boxScoreTraditional', {})
            
            for team_key in ['homeTeam', 'awayTeam']:
                team_data = box.get(team_key, {})
                team_id = team_data.get('teamId')
                team_slug = team_data.get('teamTricode')
                
                # 1. TEAM STATS
                t_stats = team_data.get('statistics', {})
                team_row = {
                    'game_id': game_id,
                    'season': season,
                    'team_id': team_id,
                    'team_slug': team_slug,
                    'is_home': True if team_key == 'homeTeam' else False,
                    **t_stats 
                }
                all_team_stats.append(team_row)
                
                # 2. PLAYER STATS
                players = team_data.get('players', [])
                for p in players:
                    p_stats = p.get('statistics', {})
                    player_row = {
                        'game_id': game_id,
                        'season': season,
                        'team_id': team_id,
                        'team_slug': team_slug,
                        'player_id': p.get('personId'),
                        'player_name': f"{p.get('firstName')} {p.get('familyName')}",
                        'position': p.get('position'),
                        'comment': p.get('comment'),
                        **p_stats 
                    }
                    all_player_stats.append(player_row)

        except Exception as e:
            # Silent fail for individual corrupt files, but print warning
            print(f"Warning: Error reading {filename}: {e}")

    # Save
    if all_player_stats:
        pd.DataFrame(all_player_stats).to_parquet(
            os.path.join(player_out, f"player_stats_{season}.parquet"), index=False
        )
        pd.DataFrame(all_team_stats).to_parquet(
            os.path.join(team_out, f"team_stats_{season}.parquet"), index=False
        )
        print(f"✅ Saved Stats for {season}")

# --- 2. PLAY-BY-PLAY PROCESSOR ---
def process_pbp(season):
    """
    Reads Bronze PBP (CDN) and creates:
    1. fact_plays.parquet
    """
    bronze_dir = os.path.join(common.get_data_dir('bronze'), 'play_by_play', f"season={season}")
    silver_out = os.path.join(common.get_data_dir('silver'), 'fact_plays')
    os.makedirs(silver_out, exist_ok=True)

    if not os.path.exists(bronze_dir): return

    files = sorted([f for f in os.listdir(bronze_dir) if f.endswith('.json.gz')])
    print(f"Processing {len(files)} PBP files for {season}...")
    
    all_actions = []

    for filename in tqdm(files, desc="Parsing PBP"):
        game_id = filename.split('.')[0]
        filepath = os.path.join(bronze_dir, filename)
        
        try:
            with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                data = json.load(f)
            
            # CDN Structure: game -> actions
            game_data = data.get('game', {})
            actions = game_data.get('actions', [])
            
            for action in actions:
                row = {
                    'game_id': game_id,
                    'season': season,
                    'event_num': action.get('actionNumber'),
                    'period': action.get('period'),
                    'clock': action.get('clock'),
                    'action_type': action.get('actionType'),
                    'sub_type': action.get('subType'),
                    'description': action.get('description'),
                    'score_home': action.get('scoreHome'),
                    'score_away': action.get('scoreAway'),
                    'person_id': action.get('personId'),
                    'player_name': action.get('playerName'),
                    'team_id': action.get('teamId'),
                    'team_slug': action.get('teamTricode'),
                    # Tracking Data
                    'loc_x': action.get('x'),
                    'loc_y': action.get('y'),
                    'shot_distance': action.get('shotDistance'),
                    'shot_result': action.get('shotResult'),
                    'is_field_goal': action.get('isFieldGoal')
                }
                all_actions.append(row)

        except Exception as e:
            print(f"Warning: Error reading PBP {filename}: {e}")

    if all_actions:
        df = pd.DataFrame(all_actions)
        
        # Enforce numeric types for analysis
        numeric_cols = ['event_num', 'period', 'score_home', 'score_away', 'loc_x', 'loc_y', 'shot_distance']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df.to_parquet(os.path.join(silver_out, f"plays_{season}.parquet"), index=False)
        print(f"✅ Saved Plays for {season}")

# --- 3. GAME INFO PROCESSOR ---
def process_summary(season):
    """
    Reads Bronze Summary (V3) and creates:
    1. dim_game.parquet
    """
    bronze_dir = os.path.join(common.get_data_dir('bronze'), 'game_summary', f"season={season}")
    silver_out = os.path.join(common.get_data_dir('silver'), 'dim_game')
    os.makedirs(silver_out, exist_ok=True)

    if not os.path.exists(bronze_dir): return
    files = sorted([f for f in os.listdir(bronze_dir) if f.endswith('.json.gz')])
    
    all_games = []
    
    for filename in tqdm(files, desc="Parsing Summaries"):
        game_id = filename.split('.')[0]
        try:
            with gzip.open(os.path.join(bronze_dir, filename), 'rt', encoding='utf-8') as f:
                data = json.load(f)
            
            summ = data.get('boxScoreSummary', {})
            officials = summ.get('officials', [])
            
            row = {
                'game_id': game_id,
                'season': season,
                'game_date': summ.get('gameDate'),
                'game_status': summ.get('gameStatusText'),
                'arena': summ.get('arena', {}).get('arenaName'),
                'attendance': summ.get('attendance'),
                'duration': summ.get('duration'),
                # Extract first 3 refs if available
                'official_1': f"{officials[0].get('firstName')} {officials[0].get('familyName')}" if len(officials) > 0 else None,
                'official_2': f"{officials[1].get('firstName')} {officials[1].get('familyName')}" if len(officials) > 1 else None,
                'official_3': f"{officials[2].get('firstName')} {officials[2].get('familyName')}" if len(officials) > 2 else None,
            }
            all_games.append(row)
        except: pass

    if all_games:
        pd.DataFrame(all_games).to_parquet(os.path.join(silver_out, f"games_{season}.parquet"), index=False)
        print(f"✅ Saved Game Info for {season}")

# --- 4. PLAYER DIMENSION PROCESSOR ---
def process_players(season):
    """
    Extracts unique player info from Boxscores.
    Updates logic: Last Team Played = Player's Team for the season.
    """
    bronze_dir = os.path.join(common.get_data_dir('bronze'), 'boxscore', f"season={season}")
    silver_out = os.path.join(common.get_data_dir('silver'), 'dim_player')
    os.makedirs(silver_out, exist_ok=True)

    if not os.path.exists(bronze_dir): return

    # Sort files chronologically is crucial for "Latest Team" logic
    files = sorted([f for f in os.listdir(bronze_dir) if f.endswith('.json.gz')])
    
    unique_players = {} 

    for filename in tqdm(files, desc="Extracting Players"):
        try:
            with gzip.open(os.path.join(bronze_dir, filename), 'rt', encoding='utf-8') as f:
                data = json.load(f)
            
            box = data.get('boxScoreTraditional', {})
            for team_key in ['homeTeam', 'awayTeam']:
                t_data = box.get(team_key, {})
                team_id = t_data.get('teamId')
                team_slug = t_data.get('teamTricode')
                
                for p in t_data.get('players', []):
                    p_id = p.get('personId')
                    
                    # Upsert: Overwrite with latest info found
                    unique_players[p_id] = {
                        'player_id': p_id,
                        'player_name': f"{p.get('firstName')} {p.get('familyName')}",
                        'position': p.get('position'),
                        'team_id': team_id,
                        'team_slug': team_slug,
                        'season': season
                    }
        except: pass

    if unique_players:
        df = pd.DataFrame(list(unique_players.values()))
        out_path = os.path.join(silver_out, f"players_{season}.parquet")
        df.to_parquet(out_path, index=False)
        print(f"✅ Saved Player Dim for {season}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # You can loop through all seasons here if you want
    SEASON = "2019-20"
    
    print(f"--- Processing Silver Layer for {SEASON} ---")
    process_boxscore(SEASON)
    process_pbp(SEASON)
    process_summary(SEASON)
    process_players(SEASON)
    print("\nAll Silver tables updated successfully.")