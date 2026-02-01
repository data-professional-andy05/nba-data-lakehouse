import duckdb
import os
from src.utils import common

class NBA_DB:
    def __init__(self):
        # Create an in-memory DuckDB connection
        self.con = duckdb.connect(database=':memory:')
        self._register_views()

    def _register_views(self):
        """
        Auto-discovers Silver Parquet files and registers them as SQL Views.
        This allows you to query 'fact_plays' directly.
        """
        silver_root = common.get_data_dir('silver')
        
        # Define the mapping of Table Name -> Folder Name
        tables = {
            'fact_plays': 'fact_plays',
            'fact_player_stats': 'fact_player_game_stats',
            'fact_team_stats': 'fact_team_game_stats',
            'dim_game': 'dim_game',
            'dim_player': 'dim_player'
        }

        print("--- Registering Silver Tables in DuckDB ---")
        for table_name, folder in tables.items():
            # Wildcard path to read ALL seasons at once (e.g., plays_*.parquet)
            path = os.path.join(silver_root, folder, "*.parquet")
            
            # Create View
            try:
                # We use glob pattern to capture all years
                self.con.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS 
                    SELECT * FROM read_parquet('{path}')
                """)
                print(f"✅ Registered view: {table_name}")
            except Exception as e:
                print(f"⚠️ Could not register {table_name} (No data yet?): {e}")

    def query(self, sql):
        """
        Executes a SQL query and returns a Pandas DataFrame.
        """
        return self.con.execute(sql).df()

    def create_gold_table(self, table_name, sql):
        """
        Saves a query result as a persistent Gold Parquet file.
        """
        df = self.query(sql)
        gold_path = os.path.join(common.get_data_dir('gold'), f"{table_name}.parquet")
        os.makedirs(os.path.dirname(gold_path), exist_ok=True)
        
        df.to_parquet(gold_path, index=False)
        print(f"🏆 Saved Gold Table: {gold_path}")
        return df