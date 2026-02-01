import gzip
import json
import os
from src.utils import common

def print_structure(data, indent=0):
    """Recursively prints keys of a dictionary."""
    spacing = "  " * indent
    if isinstance(data, dict):
        for key in data.keys():
            print(f"{spacing}- {key}")
            # If the value is a complex object, verify inside (just one level deep)
            if indent < 1 and isinstance(data[key], dict):
                print_structure(data[key], indent + 1)
            elif indent < 1 and isinstance(data[key], list) and len(data[key]) > 0:
                print(f"{spacing}  [List of objects like:]")
                print_structure(data[key][0], indent + 2)

def inspect_one_file(endpoint, season="2019-20"):
    base_dir = os.path.join(common.get_data_dir('bronze'), endpoint, f"season={season}")
    
    if not os.path.exists(base_dir):
        print(f"No data found for {endpoint} in {season}")
        return

    # Pick first file
    filename = os.listdir(base_dir)[0]
    filepath = os.path.join(base_dir, filename)
    
    print(f"\n=== INSPECTING {endpoint.upper()} ({filename}) ===")
    with gzip.open(filepath, 'rt', encoding='utf-8') as f:
        data = json.load(f)
        print_structure(data)

if __name__ == "__main__":
    # Check the 3 types we saved
    inspect_one_file('boxscore')      # Should be V3 Structure
    inspect_one_file('play_by_play')  # Should be CDN Structure
    inspect_one_file('game_summary')  # Should be V3 Structure