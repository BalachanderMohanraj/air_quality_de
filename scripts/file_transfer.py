import os
import shutil
from datetime import datetime
raw_dir = "data/Raw"
archive_dir = os.path.join(raw_dir, "archive")
raw_file = os.path.join(raw_dir, "locations.csv")
os.makedirs(archive_dir, exist_ok=True)
today = datetime.today().strftime("%Y-%m-%d")
archived_file = os.path.join(archive_dir, f"locations_{today}.csv")
if os.path.exists(raw_file):
    shutil.copy2(raw_file, archived_file)
    print(f"Archived raw file to: {archived_file}")
else:
    print(f"No raw file found at: {raw_file}")
