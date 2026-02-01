import os
import json
import gzip
import time
import requests
import random
from tqdm import tqdm

# Import V3 endpoints for official stats
from nba_api.stats.endpoints import leaguegamelog, boxscoretraditionalv3, boxscoresummaryv3

# Internal project imports
from config import settings
from src.utils import common

# --- 1. VALIDATION LOGIC ---

def is_valid_file(path, min_size):
    """
    Checks if a file exists and meets the minimum size threshold in bytes.
    Ensures we don't skip over empty or 'soft-blocked' 1KB files.
    """
    if os.path.exists(path):
        return os.path.getsize(path) >= min_size
    return False

def check_game_complete(season, game_id):
    """
    Verifies that all three Bronze components exist and look like real data.
    """
    base_dir = common.get_data_dir('bronze')
    
    pbp_path = os.path.join(base_dir, 'play_by_play', f"season={season}", f"{game_id}.json.gz")
    box_path = os.path.join(base_dir, 'boxscore', f"season={season}", f"{game_id}.json.gz")
    sum_path = os.path.join(base_dir, 'game_summary', f"season={season}", f"{game_id}.json.gz")
    
    # We use tailored thresholds based on typical NBA data sizes (gzipped)
    return (is_valid_file(pbp_path, min_size=5000) and  # PBP Floor (5KB)
            is_valid_file(box_path, min_size=2000) and  # Boxscore Floor (2KB)
            is_valid_file(sum_path, min_size=1000))     # Summary Floor (1KB)

# --- 2. STORAGE LOGIC ---

def save_raw_json(data_dict, endpoint_name, season, game_id):
    """
    Saves the API dictionary response as a compressed JSON file using Hive-style partitioning.
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

# --- 3. SCRAPING LOGIC ---

def get_game_ids(season):
    """
    Fetches the full schedule for a season to identify games to download.
    """
    print(f"Fetching schedule for {season}...")
    try:
        log = leaguegamelog.LeagueGameLog(season=season, season_type_all_star='Regular Season')
        time.sleep(2) # Be extra polite during schedule fetch
        data = log.get_dict()
        
        if 'resultSets' not in data:
            return []

        games = data['resultSets'][0]['rowSet']
        headers = data['resultSets'][0]['headers']
        id_idx = headers.index('GAME_ID')
        
        return sorted(list(set([row[id_idx] for row in games])))
    except Exception as e:
        print(f"Error getting schedule for {season}: {e}")
        return []

def scrape_game(game_id, season):
    """
    Downloads data for a single game across three endpoints.
    Uses the CDN 'Backdoor' for Play-by-Play to avoid Akamai blocks.
    """
    
    # --- 1. Play by Play (CDN) ---
    # The CDN is robust, but we must strip the 'Host' header meant for stats.nba.com
    try:
        url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
        
        # Specific headers for CDN (Do NOT send 'Host: stats.nba.com')
        cdn_headers = {
            'User-Agent': settings.NBA_HEADERS['User-Agent'],
            'Referer': 'https://www.nba.com/',
            'Accept': 'application/json',
            'Connection': 'keep-alive'
        }
        
        resp = requests.get(url, headers=cdn_headers, timeout=15)
        
        if resp.status_code == 200:
            save_raw_json(resp.json(), 'play_by_play', season, game_id)
        elif resp.status_code == 403:
            print(f" > [BLOCKED] CDN 403 for PBP {game_id}")
            return False # Signal that we are blocked
        else:
            print(f" > Failed PBP {game_id} (CDN): Status {resp.status_code}")
            
    except Exception as e:
        print(f" > PBP Exception {game_id}: {e}")
    
    # --- 2. Boxscore (API V3) ---
    try:
        # We use the full header set for stats.nba.com endpoints
        box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, headers=settings.NBA_HEADERS)
        save_raw_json(box.get_dict(), 'boxscore', season, game_id)
    except Exception as e:
        print(f" > Boxscore Exception {game_id}: {e}")

    # --- 3. Summary (API V3) ---
    try:
        summary = boxscoresummaryv3.BoxScoreSummaryV3(game_id=game_id, headers=settings.NBA_HEADERS)
        save_raw_json(summary.get_dict(), 'game_summary', season, game_id)
    except Exception as e:
        print(f" > Summary Exception {game_id}: {e}")
    
    return True

# --- 4. EXECUTION LOOP ---

def run():
    for season in settings.SEASONS:
        print(f"\n--- Processing Season: {season} ---")
        game_ids = get_game_ids(season)
        
        if not game_ids:
            continue

        # SMART RESUME: Identify only games that are missing or corrupted
        games_to_scrape = [g for g in game_ids if not check_game_complete(season, g)]
        
        print(f"Total games in schedule: {len(game_ids)}")
        print(f"Already complete: {len(game_ids) - len(games_to_scrape)}")
        print(f"Remaining to download: {len(games_to_scrape)}")

        if not games_to_scrape:
            print(f"Season {season} is already fully downloaded.")
            continue

        for i, game_id in enumerate(tqdm(games_to_scrape, desc=f"Scraping {season}")):
            
            # --- BATCH COOLDOWN (The Patient Logic) ---
            # Take a long break every BATCH_SIZE games (e.g., 50)
            if i > 0 and i % settings.BATCH_SIZE == 0:
                print(f"\n[Cooldown] Batch of {settings.BATCH_SIZE} complete. Waiting {settings.BATCH_PAUSE}s...")
                time.sleep(settings.BATCH_PAUSE)
            
            # Scrape
            success = scrape_game(game_id, season)
            
            # If we get a 403 on the CDN, it's safer to stop than to burn the IP further
            if success is False:
                print("\nCRITICAL: Detected 403 Block. Stopping script safely.")
                return

            # --- RANDOMIZED JITTER ---
            # Wait Base + Random (e.g., 4s + 0-3s)
            delay = settings.REQUEST_DELAY + random.uniform(0, settings.RANDOM_JITTER)
            time.sleep(delay)

if __name__ == "__main__":
    run()