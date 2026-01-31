# src/utils/common.py

import os

# --- PATH FUNCTIONS ---

def get_project_root():
    """
    Returns the absolute path to the project root directory.
    (e.g., C:/Users/Name/Projects/nba-data-lakehouse)
    """
    # This file is in src/utils/common.py -> Go up 3 levels to find root
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_data_dir(layer='bronze'):
    """
    Returns the absolute path to a specific data layer.
    
    Args:
        layer (str): 'bronze', 'silver', or 'gold'
    
    Returns:
        str: Absolute path to the folder (e.g., .../data/bronze)
    """
    return os.path.join(get_project_root(), 'data', layer)