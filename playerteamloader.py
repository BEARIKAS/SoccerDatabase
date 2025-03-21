import os
import pandas as pd
import mysql.connector
from tqdm import tqdm  

# Directory setup
project_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(project_dir, 'data')
os.makedirs(data_dir, exist_ok=True)
os.environ['KAGGLE_CONFIG_DIR'] = project_dir  

import kaggle

# Download the players/teams dataset
print("Downloading soccer players dataset from Kaggle...")
kaggle.api.dataset_download_files('antoinekrajnc/soccer-players-statistics', path=data_dir, unzip=True)
player_csv_path = os.path.join(data_dir, "FullData.csv")
print(f"Using player file: {player_csv_path}")
players_df = pd.read_csv(player_csv_path)

# Displaying column names 
print("Columns in players dataset:", players_df.columns.tolist())

# Extract teams (clubs) for the Teams table
if 'Club' in players_df.columns:
    teams_df = players_df[['Club']].drop_duplicates().reset_index(drop=True)
    teams_df.rename(columns={'Club': 'Team_Name'}, inplace=True)
    teams_df['Team_ID'] = teams_df.index + 1
elif 'club' in players_df.columns:  # lower just in case
    teams_df = players_df[['club']].drop_duplicates().reset_index(drop=True)
    teams_df.rename(columns={'club': 'Team_Name'}, inplace=True)
    teams_df['Team_ID'] = teams_df.index + 1
else:
    print("Warning: No Club column found, trying to find any column that might contain club info")
    potential_cols = [col for col in players_df.columns if 'club' in col.lower() or 'team' in col.lower()]
    if potential_cols:
        teams_df = players_df[[potential_cols[0]]].drop_duplicates().reset_index(drop=True)
        teams_df.rename(columns={potential_cols[0]: 'Team_Name'}, inplace=True)
        teams_df['Team_ID'] = teams_df.index + 1
    else:
        # placeholder team if no club/team info is found
        teams_df = pd.DataFrame({'Team_Name': ['Unknown'], 'Team_ID': [1]})

print(f"Found {len(teams_df)} unique teams")

# direct mappings, only problematic one is potential
column_mapping = {
    'Name': 'Player_Name',              # Player_Name = Name
    'Preffered_Position': 'Position',   # Position = Preferred_Position
    'Rating': 'Overall_Rating',         # Overall_Rating = Rating
    'Ball_Control': 'Ball_Control',     # Ball_Control = Ball_Control
    'Stamina': 'Stamina',               # Stamina = Stamina
    'Composure': 'Potential',           #Linking Potential to Finishing for now
    'Short_Pass': 'Short_Passing',      # Short_Passing = Short_Pass
    'Shot_Power': 'Shot_Power',         # Shot_Power = Shot_Power
    'Agility': 'Agility',               # Agility = Agility
    'Penalties': 'Penalties',           # Penalties = Penalties
    'Freekick_Accuracy': 'Free_Kick_Accuracy', # Free_Kick_Accuracy = Freekick_Accuracy
    'Strength': 'Strength'              # Strength = Strength
}

# Check available columns and select only those that exist
available_columns = [col for col in column_mapping.keys() if col in players_df.columns]

# Create the processed dataframe with renamed columns
players_processed = players_df[available_columns].rename(columns={k: column_mapping[k] for k in available_columns})

# Add Player_ID
players_processed['Player_ID'] = players_processed.index + 1

# Add Team_ID by merging with teams_df
# Identify which column contains team/club information
team_col = None
if 'Club' in players_df.columns:
    team_col = 'Club'
elif 'club' in players_df.columns:
    team_col = 'club'
else:
    potential_cols = [col for col in players_df.columns if 'club' in col.lower() or 'team' in col.lower()]
    if potential_cols:
        team_col = potential_cols[0]

if team_col:
    # Add the team info from the original dataframe 
    players_processed['original_team'] = players_df[team_col]
    # Merge to get Team_ID
    players_processed = pd.merge(
        players_processed,
        teams_df[['Team_Name', 'Team_ID']],
        left_on='original_team', right_on='Team_Name',
        how='left'
    )
    # Clean up
    players_processed.drop(['original_team'], axis=1, inplace=True)
    if 'Team_Name' in players_processed.columns:
        players_processed.drop('Team_Name', axis=1, inplace=True)
else:
    # If no team column found, assign all to team ID 1
    players_processed['Team_ID'] = 1

# To fill in for missing data
numeric_columns = [
    'Overall_Rating', 'Ball_Control', 'Stamina', 'Potential',
    'Short_Passing', 'Shot_Power', 'Agility', 'Penalties',
    'Free_Kick_Accuracy', 'Strength'
]

for col in numeric_columns:
    if col in players_processed.columns:
        players_processed[col] = players_processed[col].fillna(50).astype(int)
    else:
        players_processed[col] = 50  # Is this better than null?

if 'Position' in players_processed.columns:
    players_processed['Position'] = players_processed['Position'].astype(str)
else:
    players_processed['Position'] = 'Unknown'  # no Preffered_Position

# Sample of data
print("\nProcessed player data sample:")
print(players_processed.head(3))
print(f"Total players: {len(players_processed)}")

# Databse connection
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'uiuc',
    'database': 'soccer'
}

# Connect to MySQL
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    # 1. Insert Teams data
    print("Importing Teams data...")
    teams_inserted = 0
    for index, row in tqdm(teams_df.iterrows(), total=len(teams_df)):
        # checking if team exists
        check_query = "SELECT Team_ID FROM Teams WHERE Team_Name = %s"
        cursor.execute(check_query, (row['Team_Name'],))
        result = cursor.fetchone()
        
        if result is None:  # Team doesn't exist yet
            query = "INSERT INTO Teams (Team_ID, Team_Name) VALUES (%s, %s)"
            values = (
                int(row['Team_ID']),
                row['Team_Name']
            )
            cursor.execute(query, values)
            teams_inserted += 1
    
    # 2. Insert Players data
    print("Importing Players data...")
    players_inserted = 0
    for index, row in tqdm(players_processed.iterrows(), total=len(players_processed)):
        # Check if player already exists
        check_query = "SELECT Player_ID FROM Players WHERE Player_ID = %s"
        cursor.execute(check_query, (int(row['Player_ID']),))
        result = cursor.fetchone()
        
        if result is None:  # Player doesn't exist yet
            query = """
            INSERT INTO Players 
            (Player_ID, Player_Name, Team_ID, Position, Overall_Rating, 
             Ball_Control, Stamina, Potential, Short_Passing, Shot_Power, 
             Agility, Penalties, Free_Kick_Accuracy, Strength)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                int(row['Player_ID']),
                str(row['Player_Name']) if not pd.isna(row['Player_Name']) else "Unknown Player",
                int(row['Team_ID']) if not pd.isna(row['Team_ID']) else None,
                str(row['Position']),
                int(row['Overall_Rating']),
                int(row['Ball_Control']),
                int(row['Stamina']),
                int(row['Potential']),
                int(row['Short_Passing']),
                int(row['Shot_Power']),
                int(row['Agility']),
                int(row['Penalties']),
                int(row['Free_Kick_Accuracy']),
                int(row['Strength'])
            )
            
            try:
                cursor.execute(query, values)
                players_inserted += 1
            except mysql.connector.Error as err:
                print(f"Error inserting player {row['Player_ID']}: {err}")
                print(f"Values: {values}")
    
    # Final commit to database
    conn.commit()
    
    print(f"Successfully imported {teams_inserted} team records and {players_inserted} player records")
except mysql.connector.Error as err:
    print(f"Database error: {err}")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed")