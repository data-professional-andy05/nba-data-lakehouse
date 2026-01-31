import os

def create_structure():
    folders = [
        "data/bronze",
        "data/silver",
        "data/gold",
        "notebooks",
        "src/ingestion",
        "src/processing",
        "src/analysis",
        "src/utils",
        "config" # For config files or SQL scripts
    ]
    
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        # Create a .gitkeep file so Git tracks empty folders
        with open(os.path.join(folder, ".gitkeep"), "w") as f:
            pass
        print(f"Created: {folder}")

if __name__ == "__main__":
    create_structure()