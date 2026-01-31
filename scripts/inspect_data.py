import gzip
import json
import os
import pprint

def inspect_file(layer_name, season="2024-25"):
    base_dir = f"data/bronze/{layer_name}/season={season}"
    
    if not os.path.exists(base_dir):
        print(f"❌ Directory not found: {base_dir}")
        return

    files = os.listdir(base_dir)
    if not files:
        print(f"❌ No files found in {layer_name}")
        return

    # Pick the first file
    file_path = os.path.join(base_dir, files[0])
    print(f"\n--- INSPECTING: {layer_name.upper()} ({files[0]}) ---")
    print(f"File Size: {os.path.getsize(file_path)} bytes")

    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            data = json.load(f)
            
        # --- 1. Check for Play by Play Data ---
        if layer_name == 'play_by_play':
            if 'resultSets' in data:
                rows = data['resultSets'][0]['rowSet']
                print(f"✅ Row Count: {len(rows)}")
                if len(rows) > 0:
                    print("Sample Row:", rows[0])
                else:
                    print("⚠️ WARNING: RowSet is empty!")
            else:
                print("⚠️ WARNING: 'resultSets' key missing. Keys found:", list(data.keys()))

        # --- 2. Check for Boxscore V3 Structure ---
        elif layer_name == 'boxscore':
            if 'boxScoreTraditional' in data:
                print("✅ Structure: V3 (boxScoreTraditional detected)")
                # Check if it actually has player data
                home_players = data['boxScoreTraditional'].get('homeTeam', {}).get('players', [])
                print(f"Home Team Players: {len(home_players)}")
            elif 'resultSets' in data:
                print("ℹ️ Structure: V2 (resultSets detected)")
            else:
                print("⚠️ WARNING: Unknown structure. Keys:", list(data.keys()))

    except Exception as e:
        print(f"❌ Error reading file: {e}")

if __name__ == "__main__":
    # Check the two critical files
    inspect_file("play_by_play")
    inspect_file("boxscore")