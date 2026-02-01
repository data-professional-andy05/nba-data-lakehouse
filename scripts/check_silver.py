import pandas as pd
import os
from src.utils import common

def check_file(folder, filename):
    path = os.path.join(common.get_data_dir('silver'), folder, filename)
    if not os.path.exists(path):
        print(f"❌ File not found: {folder}/{filename}")
        return

    print(f"\n--- INSPECTING: {folder} ---")
    df = pd.read_parquet(path)
    
    print(f"Dimensions: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Save a tiny sample to CSV for human inspection
    sample_csv = f"sample_{folder}.csv"
    df.head(100).to_csv(sample_csv, index=False)
    print(f"📝 Saved sample to {sample_csv} (Open in Excel to verify)")

if __name__ == "__main__":
    season = "2019-20"
    
    # Check the main tables
    check_file("fact_player_game_stats", f"player_stats_{season}.parquet")
    check_file("fact_team_game_stats", f"team_stats_{season}.parquet")
    check_file("fact_plays", f"plays_{season}.parquet")
    check_file("dim_game", f"games_{season}.parquet")
    
    # Check the missing table
    check_file("dim_player", f"players_{season}.parquet")