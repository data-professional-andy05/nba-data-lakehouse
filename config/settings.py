# config/settings.py

# --- SCRAPING CONFIGURATION ---

# The seasons we want to process
SEASONS = ['2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']

# Constraints to avoid getting banned
REQUEST_DELAY = 1.0   # Seconds to wait between API calls
TIMEOUT = 30          # Seconds to wait for a response before giving up

# --- HEADERS ---
# These are the "Good Headers" from your original script.
# We keep them here in case we need to inject them into nba_api later.
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