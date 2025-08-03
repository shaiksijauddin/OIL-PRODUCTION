import sqlite3
import pandas as pd
from pathlib import Path

# 1. Connect to SQLite database
db_path = 'oil_production.db'  # Path to your .db file
conn = sqlite3.connect(db_path)

# 2. Get list of all tables in the database
query = "SELECT name FROM sqlite_master WHERE type='table';"
tables = pd.read_sql(query, conn)['name'].tolist()

# 3. Export each table to CSV
output_folder = Path('csv_exports')
output_folder.mkdir(exist_ok=True)  # Create folder if it doesn't exist

for table in tables:
    # Read table into DataFrame
    df = pd.read_sql(f'SELECT * FROM {table}', conn)
    
    # Save to CSV
    csv_path = output_folder / f'{table}.csv'
    df.to_csv(csv_path, index=False)
    print(f"Exported {table} to {csv_path}")

# 4. Close connection
conn.close()
