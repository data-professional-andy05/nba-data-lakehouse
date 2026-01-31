import os
import json
import gzip
import time
import requests
from tqdm import tqdm

# Import V3 endpoints for Boxscore/Summary
from nba_api.stats.endpoints import leaguegamelog, boxscoretraditionalv3, boxscoresummaryv3

from config import settings
from src.utils import common

def save_raw_json(data_dict, endpoint_name, season, game_id):
    """
    Saves the API dictionary response as a compressed JSON file.
    """
    base_dir = common.get_data_dir('bronze')
    save_dir = os.path.join(base_dir, endpoint_name, f"season={season}")
    os.makedirs(save_dir, exist_ok=True)
    
    file_path = os.path.join(save_dir, f"{game_id}.json.gz")
    
    try:
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            json.dump(data_dict, f)
    except Exception as e:
        print(f"Error saving {file_path}: {e}")

def get_game_ids(season):
    """
    Fetches game IDs using the LeagueGameLog endpoint.
    """
    print(f"Fetching schedule for {season}...")
    try:
        # Fetch Regular Season games
        log = leaguegamelog.LeagueGameLog(season=season, season_type_all_star='Regular Season')
        time.sleep(1) 
        data = log.get_dict()
        
        if 'resultSets' not in data:
            return []

        games = data['resultSets'][0]['rowSet']
        headers = data['resultSets'][0]['headers']
        try:
            id_idx = headers.index('GAME_ID')
        except ValueError:
            return []
        
        game_ids = [row[id_idx] for row in games]
        return sorted(list(set(game_ids)))
        
    except Exception as e:
        print(f"Error getting schedule for {season}: {e}")
        return []

def scrape_game(game_id, season):
    """
    Hybrid Scraper:
    1. PBP -> CDN (Stable, fast, unblocked)
    2. Boxscore -> API (Official stats)
    3. Summary -> API (Metadata)
    """
    
    # --- 1. Play by Play (CDN SOURCE) ---
    try:
        # The CDN URL is static and robust
        url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
        
        # We don't need complex headers for CDN, just a polite User-Agent
        cdn_headers = {
            'User-Agent': settings.NBA_HEADERS['User-Agent'],
            'Referer': 'https://www.nba.com/'
        }
        
        resp = requests.get(url, headers=cdn_headers, timeout=10)
        
        if resp.status_code == 200:
            save_raw_json(resp.json(), 'play_by_play', season, game_id)
        else:
            print(f"Failed PBP {game_id} (CDN): Status {resp.status_code}")
            
    except Exception as e:
        print(f"Failed PBP for {game_id}: {e}")
    
    # --- 2. Boxscore (API V3) ---
    try:
        # We use the API for Boxscores because it's the official record
        box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, headers=settings.NBA_HEADERS)
        save_raw_json(box.get_dict(), 'boxscore', season, game_id)
    except Exception as e:
        print(f"Failed Boxscore for {game_id}: {e}")

    # --- 3. Summary (API V3) ---
    try:
        summary = boxscoresummaryv3.BoxScoreSummaryV3(game_id=game_id, headers=settings.NBA_HEADERS)
        save_raw_json(summary.get_dict(), 'game_summary', season, game_id)
    except Exception as e:
        print(f"Failed Summary for {game_id}: {e}")
        
    # Small sleep to be polite to the API (CDN doesn't care much, but API does)
    time.sleep(0.5)

def run():
    for season in settings.SEASONS:
        print(f"\n--- Starting Season: {season} ---")
        game_ids = get_game_ids(season)
        
        if not game_ids:
            continue

        print(f"Found {len(game_ids)} games.")
        
        # --- TEST MODE ---
        # Run only 5 games per season to verify the pipeline.
        # Once verified, comment out the [:5] to run everything.
        test_games = game_ids[:5]
        
        for game_id in tqdm(test_games, desc=f"Scraping {season}"):
            scrape_game(game_id, season)

if __name__ == "__main__":
    run()