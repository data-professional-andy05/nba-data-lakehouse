# config/settings.py

import os

# --- SEASONS TO PROCESS ---
# We keep this list for now. Later we can add the helper function to generate 1996-2018.
SEASONS = ['2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']

# --- NETWORK HEADERS ---
# These are your "Good Headers" that worked. Keep them.
NBA_HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Host': 'stats.nba.com',
    'Origin': 'https://www.nba.com',
    'Referer': 'https://www.nba.com/',
    'Sec-Ch-Ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
}

# --- SCRAPING CONSTRAINTS (Patient Mode) ---
# We update these to be conservative for future runs.
REQUEST_DELAY = 4.0   # Base seconds to wait
RANDOM_JITTER = 3.0   # Up to +3 seconds random
BATCH_SIZE = 50       # Number of games before a long pause
BATCH_PAUSE = 60      # Seconds for the "coffee break"
TIMEOUT = 30