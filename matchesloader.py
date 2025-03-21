import pandas as pd
import mysql.connector
import subprocess
import sys
from pathlib import Path


# Database Connection
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "uiuc",
    "database": "soccer"
}

# GitHub repository information
REPO_URL = "https://github.com/schochastics/football-data.git"
REPO_PATH = Path("./football-data")
PARQUET_FILE_PATH = REPO_PATH / "data" / "results" / "games.parquet"

def connect_to_db():
    """Establish connection to the MySQL database"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("Successfully connected to the database")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to the database: {err}")
        sys.exit(1)

def get_team_mapping(conn):
    """Create a mapping of team names to team IDs from the database"""
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT Team_ID, Team_Name FROM Teams")
    teams = cursor.fetchall()
    cursor.close()
    
    # mapping from team name to team ID
    team_mapping = {team['Team_Name']: team['Team_ID'] for team in teams}
    return team_mapping

def clone_repository():
    """Clone the GitHub repository and return the path to the parquet file"""
    try:
        # checking if repository already exists
        if not REPO_PATH.exists():
            print(f"Cloning repository {REPO_URL}...")
            subprocess.run(["git", "clone", REPO_URL], check=True)
        else:
            print(f"Repository already exists, pulling latest changes...")
            subprocess.run(["git", "-C", str(REPO_PATH), "pull"], check=True)
        
        # checking if file exists
        if not PARQUET_FILE_PATH.exists():
            print(f"Error: Parquet file not found at {PARQUET_FILE_PATH}")
            sys.exit(1)
            
        print(f"Successfully located parquet file at {PARQUET_FILE_PATH}")
        return PARQUET_FILE_PATH
    
    except subprocess.CalledProcessError as e:
        print(f"Error with git operation: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

def normalize_team_name(name):
    #Normalizing team names to help matching
    if name is None:
        return ""
    
    try:
        return str(name).lower().strip()
    except Exception as e:
        print(f"Error normalizing team name '{name}': {e}")
        return ""

def find_best_match(team_name, team_mapping):
    #Finding the best match for team name in the mapping with error handling
    try:
        normalized_name = normalize_team_name(team_name)
        
        # First try exact match with normalized names
        normalized_mapping = {normalize_team_name(k): v for k, v in team_mapping.items()}
        if normalized_name in normalized_mapping:
            return normalized_mapping[normalized_name]
        
        # Not completely implemented, instead of returning none, this is where we could make a better matching system. With current system we lose out on over 7,000 teams, but another reason is the dataset from where we pull teams from (kaggle) only gives us 634 teams.
        return None
    
    except Exception as e:
        print(f"Error finding match for team name '{team_name}': {e}")
        return None

def import_matches(conn):
    #Import matches from parquet file to the database
    # Get team mapping
    team_mapping = get_team_mapping(conn)
    
    # Clone repository and get parquet file path
    parquet_file_path = clone_repository()
    
    # Reading parquet
    try:
        matches_df = pd.read_parquet(parquet_file_path)
        print(f"Successfully read {len(matches_df)} matches from {parquet_file_path}")
        
        # NA values check
        for col in ['home', 'away', 'date', 'competition', 'gh', 'ga']:
            if col in matches_df.columns:
                null_count = matches_df[col].isna().sum()
                if null_count > 0:
                    print(f"Warning: Column '{col}' contains {null_count} null values")
                    # Fill NA values with empty string or 0
                    if col in ['home', 'away', 'date', 'competition']:
                        matches_df[col] = matches_df[col].fillna("")
                    else:
                        matches_df[col] = matches_df[col].fillna(0)
        
    except Exception as e:
        print(f"Error reading parquet file: {e}")
        return
    
    # Prepare cursor for insertion
    cursor = conn.cursor()
    
    # First, check if the AUTO_INCREMENT is working correctly
    cursor.execute("SHOW CREATE TABLE Matches")
    result = cursor.fetchone()
    create_table_stmt = result[1] if len(result) > 1 else result[0]
    
    print("Doing AUTO_INCREMENT here through python")
    cursor.execute("SELECT MAX(Match_ID) FROM Matches")
    max_id = cursor.fetchone()[0]
    next_id = 1 if max_id is None else max_id + 1
    print(f"Starting with Match_ID: {next_id}")
    
    insert_query = """
    INSERT INTO Matches (Match_ID, Match_Date, Match_Tournament, Home_Goals, Away_Goals, Home_Team, Away_Team)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    # Process and insert matches
    matches_inserted = 0
    matches_skipped = 0
    errors_count = 0
    team_not_found = set()
    
    # For the manual ID approach if needed
    current_id = next_id if 'next_id' in locals() else None
    
    # Loop with error handling
    for index, match in matches_df.iterrows():
        try:
            # safety checks
            home_team_name = match.get('home', None)
            away_team_name = match.get('away', None)
            
            # Skip if either team name is missing or empty
            if not home_team_name or not away_team_name:
                matches_skipped += 1
                continue
                
            # Look up team IDs, still to be implemented if the current matching is not good enough (line 79)
            home_team_id = find_best_match(home_team_name, team_mapping)
            away_team_id = find_best_match(away_team_name, team_mapping)
            
            # Skip if either team ID is not found
            if not home_team_id or not away_team_id:
                matches_skipped += 1
                # Track team names that couldn't be matched
                if not home_team_id:
                    team_not_found.add(str(home_team_name))
                if not away_team_id:
                    team_not_found.add(str(away_team_name))
                continue
            
            # Extract match details with mapping and safety checks
            match_date = str(match.get('date', ''))
            match_tournament = str(match.get('competition', ''))
            
            # Handle numeric fields safely
            try:
                home_goals = int(match.get('gh', 0))
            except (ValueError, TypeError):
                home_goals = 0
                
            try:
                away_goals = int(match.get('ga', 0))
            except (ValueError, TypeError):
                away_goals = 0
            
            # Insert match into database
            try:
                if current_id is not None:
                    # Using manual ID approach
                    cursor.execute(insert_query, (
                        current_id,
                        match_date, 
                        match_tournament, 
                        home_goals, 
                        away_goals, 
                        home_team_id, 
                        away_team_id
                    ))
                    current_id += 1
                else:
                    # Using AUTO_INCREMENT with NULL
                    cursor.execute(insert_query, (
                        match_date, 
                        match_tournament, 
                        home_goals, 
                        away_goals, 
                        home_team_id, 
                        away_team_id
                    ))
                    
                matches_inserted += 1
                errors_count = 0  # Reset consecutive errors
                
                # Print progress every 1000 matches, total with current settings is 57,347 matches (being input) and missing 1,118,588 matches
                if matches_inserted % 1000 == 0:
                    print(f"Inserted {matches_inserted} matches so far...")
                    conn.commit()
                    
            except mysql.connector.Error as err:
                print(f"Error inserting match at index {index}: {err}")
                matches_skipped += 1
                errors_count += 1
                
                # stop infinite loop if it occurs
                if errors_count > 10:
                    print("Too many consecutive errors, stopping import.")
                    break
                    
        except Exception as e:
            print(f"Unexpected error processing match at index {index}: {e}")
            matches_skipped += 1
            errors_count += 1
            
            if errors_count > 10:
                print("Too many consecutive errors, stopping import.")
                break
    
    # Final commit for any remaining changes
    try:
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Error on final commit: {err}")
    
    cursor.close()
    
    print(f"Import complete. Inserted {matches_inserted} matches. Skipped {matches_skipped} matches.")
    
    if team_not_found:
        print(f"\nCould not find {len(team_not_found)} teams in the database:")
        team_list = sorted(list(team_not_found))
        for team in team_list[:10]:  # first 10 unmatched teams
            print(f"- {team}")
        if len(team_not_found) > 10:
            print(f"... and {len(team_not_found) - 10} more.")


#Final mathces commit to database
def main():
    try:
        conn = connect_to_db()
        import_matches(conn)
        conn.close()
        print("Database connection closed.")
        
    except Exception as e:
        print(f" error in main function: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()