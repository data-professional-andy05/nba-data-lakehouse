# src/utils/common.py

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import settings  # Import the configuration we just created

# --- PATH FUNCTIONS ---

def get_project_root():
    """Returns the absolute path to the project root directory."""
    # This file is in src/utils/common.py -> Go up 3 levels
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_data_dir(layer='bronze'):
    """
    Returns the absolute path to a data layer.
    Args:
        layer (str): 'bronze', 'silver', or 'gold'
    """
    return os.path.join(get_project_root(), 'data', layer)

# --- NETWORK FUNCTIONS ---

def create_session():
    """
    Creates a requests.Session using configuration from settings.py
    """
    session = requests.Session()
    # Use headers from settings
    session.headers.update(settings.NBA_HEADERS)

    retry_strategy = Retry(
        total=settings.MAX_RETRIES,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session