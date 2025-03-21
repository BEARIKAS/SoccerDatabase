import os
import pandas as pd
import mysql.connector
from tqdm import tqdm  
import re  

# Direcory setup
project_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(project_dir, 'data')
os.makedirs(data_dir, exist_ok=True)
os.environ['KAGGLE_CONFIG_DIR'] = project_dir 

import kaggle

# Download stadiums dataset
print("Downloading football stadiums dataset from Kaggle...")
kaggle.api.dataset_download_files('imtkaggleteam/football-stadiums', path=data_dir, unzip=True)
print("Files downloaded:")
for file in os.listdir(data_dir):
    print(f" - {file}")
stadium_csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv') and 'stadium' in f.lower()]

# Using the exact name of the file
stadium_csv_path = os.path.join(data_dir, "Football Stadiums.csv")
print(f"Using stadium file: {stadium_csv_path}")
stadiums_df = pd.read_csv(stadium_csv_path)

# Displaying column names 
print("Columns in stadiums dataset:", stadiums_df.columns.tolist())

# direct mapping 
column_mapping = {
    'Stadium': 'Stadium_Name',  
    'Country': 'Stadium_Country',
    'Confederation': 'Stadium_Confederation',
    'City': 'Stadium_City'
}

# Checking if HomeTeams column exists
home_teams_col = None
for col in stadiums_df.columns:
    if ('home' in col.lower() and 'team' in col.lower()) or ('club' in col.lower()):
        home_teams_col = col
        print(f"Found home teams column: {col}")
        break

# Create a normalized version of home teams column for better matching
if home_teams_col:
    print(f"Sample values from {home_teams_col} column:")
    print(stadiums_df[home_teams_col].dropna().head(3).tolist())

# Check if expected columns exist and create a new mapping for columns that exist
actual_mapping = {}
for original, target in column_mapping.items():
    if original in stadiums_df.columns:
        actual_mapping[original] = target
    else:
        print(f"Warning: Expected column '{original}' not found in dataset")

# Create the processed dataframe with renamed columns
available_columns = list(actual_mapping.keys())
if available_columns:
    columns_to_use = available_columns.copy()
    # Add home teams column if found
    if home_teams_col and home_teams_col not in columns_to_use:
        columns_to_use.append(home_teams_col)
        
    stadiums_processed = stadiums_df[columns_to_use].copy()
    # Rename only the mapped columns
    stadiums_processed.rename(columns=actual_mapping, inplace=True)
    
    # Add Stadium_ID
    stadiums_processed['Stadium_ID'] = stadiums_processed.index + 1
    
    # Fill NA
    stadiums_processed['Stadium_Name'] = stadiums_processed['Stadium_Name'].fillna('Unknown Stadium')
    stadiums_processed['Stadium_Country'] = stadiums_processed['Stadium_Country'].fillna('Unknown')
    stadiums_processed['Stadium_City'] = stadiums_processed['Stadium_City'].fillna('Unknown')
    
    # Confederation default = UNK
    if 'Stadium_Confederation' not in stadiums_processed.columns:
        stadiums_processed['Stadium_Confederation'] = 'Unknown'
    else:
        stadiums_processed['Stadium_Confederation'] = stadiums_processed['Stadium_Confederation'].fillna('Unknown')
else:
    print("ERROR found no columns")

# Display sample of data
print("\nProcessed stadium data sample:")
print(stadiums_processed.head(3))
print(f"Total stadiums: {len(stadiums_processed)}")

# Database connection 
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uiuc',
    'database': 'soccer'
}

# Function to normalize team names for comparison
def normalize_team_name(name):
    if pd.isna(name):
        return ""
    # Convert to lowercase, remove non-alphanumeric chars except spaces
    name = str(name).lower().strip()
    name = re.sub(r'[^\w\s]', '', name)
    # Replace multiple spaces with single space
    name = re.sub(r'\s+', ' ', name)
    return name

# Connect to MySQL
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    # 1. Get all teams from database first
    cursor.execute("SELECT Team_ID, Team_Name FROM Teams")
    db_teams = cursor.fetchall()
    
    # Create normalized versions of team names for better matching
    db_teams_dict = {}
    db_teams_norm = {}
    
    for team_id, team_name in db_teams:
        db_teams_dict[team_id] = team_name
        # Store normalized team name for better matching
        db_teams_norm[normalize_team_name(team_name)] = team_id
    
    print(f"Loaded {len(db_teams)} teams from database")
    print("Sample team names:", list(db_teams_dict.values())[:3])
    
    # 2. Insert Stadiums data
    print("Importing Stadiums data...")
    stadiums_inserted = 0
    
    # Create a mapping of stadium IDs to home teams
    stadium_teams_map = {}
    
    for index, row in tqdm(stadiums_processed.iterrows(), total=len(stadiums_processed)):
        # First check if the stadium already exists to avoid duplicates
        check_query = "SELECT Stadium_ID FROM Stadiums WHERE Stadium_Name = %s AND Stadium_City = %s"
        cursor.execute(check_query, (row['Stadium_Name'], row['Stadium_City']))
        result = cursor.fetchone()
        
        stadium_id = None
        if result is None:  # Stadium doesn't exist yet
            query = """
            INSERT INTO Stadiums (Stadium_ID, Stadium_Name, Stadium_Country, Stadium_Confederation, Stadium_City) 
            VALUES (%s, %s, %s, %s, %s)
            """
            values = (
                int(row['Stadium_ID']),
                str(row['Stadium_Name']),
                str(row['Stadium_Country']),
                str(row['Stadium_Confederation']),
                str(row['Stadium_City'])
            )
            try:
                cursor.execute(query, values)
                stadiums_inserted += 1
                stadium_id = int(row['Stadium_ID'])
            except mysql.connector.Error as err:
                print(f"Error inserting stadium {row['Stadium_ID']}: {err}")
        else:
            stadium_id = result[0]
        
        # Store mapping of stadium to home teams if the column exists
        if home_teams_col and stadium_id is not None:
            home_teams = row.get(home_teams_col)
            if pd.notna(home_teams) and home_teams:
                # Split teams by comma
                for separator in [',']:
                    if separator in str(home_teams):
                        teams_list = [team.strip() for team in str(home_teams).split(separator)]
                        stadium_teams_map[stadium_id] = teams_list
                        break
                else:
                    # If no separator found, treat as a single team
                    stadium_teams_map[stadium_id] = [str(home_teams).strip()]
    
    # 3. Update Teams with Home_Stadium based on the mapping
    print("Assigning home stadiums to teams...")
    updates = 0
    
    # First: Create a mapping of normalized stadium team names to stadium IDs for better matching
    stadium_team_to_id = {}
    if stadium_teams_map:
        for stadium_id, team_names in stadium_teams_map.items():
            for team_name in team_names:
                norm_name = normalize_team_name(team_name)
                if norm_name:  # Only add if not empty
                    stadium_team_to_id[norm_name] = stadium_id
    
    # Print sample of stadium team names for debugging
    if stadium_team_to_id:
        print("Sample normalized stadium team names:", list(stadium_team_to_id.keys())[:3])
    
    # First pass: Try direct normalized name matching
    teams_matched = set()
    for team_id, team_name in db_teams:
        norm_team = normalize_team_name(team_name)
        
        # Try exact match with normalized names
        if norm_team in stadium_team_to_id:
            stadium_id = stadium_team_to_id[norm_team]
            cursor.execute("""
                UPDATE Teams SET Home_Stadium = %s
                WHERE Team_ID = %s
            """, (stadium_id, team_id))
            updates += 1
            teams_matched.add(team_id)
            continue
        
        # Try substring matching
        for stadium_team, stadium_id in stadium_team_to_id.items():
            # Check if database team name is a substring of stadium team name
            if len(norm_team) > 3 and norm_team in stadium_team:
                cursor.execute("""
                    UPDATE Teams SET Home_Stadium = %s
                    WHERE Team_ID = %s
                """, (stadium_id, team_id))
                updates += 1
                teams_matched.add(team_id)
                break
            
            # Check if stadium team name is a substring of database team name
            if len(stadium_team) > 3 and stadium_team in norm_team:
                cursor.execute("""
                    UPDATE Teams SET Home_Stadium = %s
                    WHERE Team_ID = %s
                """, (stadium_id, team_id))
                updates += 1
                teams_matched.add(team_id)
                break
    
    # Second pass: For teams without a stadium, try stadium name matching
    for team_id in set(db_teams_dict.keys()) - teams_matched:
        team_name = db_teams_dict[team_id]
        
        # Try to match team with stadium based on name
        cursor.execute("""
            SELECT Stadium_ID FROM Stadiums 
            WHERE Stadium_Name LIKE %s 
            LIMIT 1
        """, (f"%{team_name}%",))
        stadium = cursor.fetchone()
        
        if not stadium:
            # Try to find if any stadium is in the same city as might be in the team name
            team_parts = team_name.split()
            for part in team_parts:
                if len(part) > 3:  # Only try with meaningful parts
                    cursor.execute("""
                        SELECT Stadium_ID FROM Stadiums 
                        WHERE Stadium_City LIKE %s OR Stadium_Name LIKE %s
                        LIMIT 1
                    """, (f"%{part}%", f"%{part}%"))
                    stadium = cursor.fetchone()
                    if stadium:
                        break
        
        # If still no match, assign a random stadium
        if not stadium:
            cursor.execute("SELECT Stadium_ID FROM Stadiums ORDER BY RAND() LIMIT 1")
            stadium = cursor.fetchone()
        
        # Update the team with the home stadium
        if stadium:
            cursor.execute("""
                UPDATE Teams SET Home_Stadium = %s
                WHERE Team_ID = %s
            """, (stadium[0], team_id))
            updates += 1
    
    # Commit changes and close connection
    conn.commit()
    
    print(f"Successfully imported {stadiums_inserted} stadium records and updated {updates} team records")
except mysql.connector.Error as err:
    print(f"Database error: {err}")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed")