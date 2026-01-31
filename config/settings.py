# config/settings.py

import os

# --- SCRAPING CONFIG ---
# Seasons to scrape by default
SEASONS = ['2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']

# Headers to mimic a real browser
NBA_HEADERS = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Host': 'stats.nba.com',
    'Origin': 'https://www.nba.com',
    'Referer': 'https://www.nba.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
}

# Network constraints
REQUEST_DELAY = 1.0  # Seconds between requests
MAX_RETRIES = 3
TIMEOUT = 30