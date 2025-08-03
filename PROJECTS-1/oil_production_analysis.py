import pandas as pd
import numpy as np
import sqlite3
from pathlib import Path

# 1. Load the data
file_path = r"P:\DOCUMENTS\PROJECTS-1\OIL_PRODUCTION.csv"
df = pd.read_csv(file_path)

# 2. Data Cleaning and Exploration
print(df.head())
print(df.info())
print(df.describe())

# Handle missing values if any
df.fillna(0, inplace=True)

# 3. Advanced Analysis with Pandas/Numpy
# Calculate annual global production
global_production = df.groupby('Year')['Oil production (TWh)'].sum().reset_index()

# Calculate production growth rates
global_production['Growth Rate'] = global_production['Oil production (TWh)'].pct_change() * 100

# Top 10 producing countries in the most recent year
latest_year = df['Year'].max()
top_producers = df[df['Year'] == latest_year].nlargest(10, 'Oil production (TWh)')

# 4. Store in SQLite Database
conn = sqlite3.connect('oil_production.db')

# Save data to SQLite
df.to_sql('oil_production', conn, if_exists='replace', index=False)
global_production.to_sql('global_production', conn, if_exists='replace', index=False)
top_producers.to_sql('top_producers', conn, if_exists='replace', index=False)

# Create some views for Tableau
cursor = conn.cursor()
cursor.execute("""
CREATE VIEW IF NOT EXISTS country_trends AS
SELECT Entity, Code, Year, [Oil production (TWh)]
FROM oil_production
WHERE Code != ''  -- Exclude regional aggregates
""")

cursor.execute("""
CREATE VIEW IF NOT EXISTS regional_production AS
SELECT Entity, Year, [Oil production (TWh)]
FROM oil_production
WHERE Code = ''  -- Only regional aggregates
""")

conn.commit()
conn.close()

print("Data processing complete. SQLite database created.")
