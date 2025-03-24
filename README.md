# SoccerDatabase
Soccer Database for soccer application later on
**Steps to just have the database locally**
1. Download soccerpopulated.sql
2. create a new database in your terminal called soccer (**enter** "mysql -u root -p", entere your password, and **then** create a new database "CREATE DATABASE soccer;")
3. Download the SQLTools and SQLTools driver extensions in vscode
4. create a new connection, called whatever, but fill in database (soccer), username (root), and password (choose the save as plaintext option)
5. click test connection
6. click save connection
7. click connect now
8. click run on active connection at the top of soccerpopulated.sql, it may ask you to choose the connections you just created
9. database should be live and you can connect to it in terminal




**Steps to run locally to insert data yourself** (need python and mysql installed)
1. Have mysql installed, remember your password and username
2. In VScode, install SQLTools and the first driver that comes up when you search SQLTools Driver in extensions
3. Now in your cmd or terminal enter "mysql -u root -p" and create a new database "CREATE DATABASE soccer;" 
4. Name this database soccer if you want to change the code minimally
5. back to VScode go to the SQLTools extension, it should be a cylinder on the lefthand side of your vscode toolbar, then click add new connection
6. Click the mysql option
7. Leave everything as default, but you need a Connection Name which should be naturaljoin (before I said it could be whatever, but when it is naturaljoin, everything becomes easier), database should be soccer, username should be your username or root (only root worked for me), change the password option to "save as plaintext" and enter the password below it 
8. Click test connection, if it doesn't work it is probably a password or username error, if anything else, maybe the database name
9. click save connection
10. click open connection
11. copy paste the naturaljoin code into your new SoccerDatabase.session
12. Run the Teams, Players, Matches, Stadiums blocks
13. run "pip install pandas kaggle mysql-connector-python pyarrow" in terminal
14. in each python file there is database connection code called db_config in json format, you probably need to change the password, possibly the username and database name as well (root and soccer work if you followed these instructions exactly)
15. Run the Python files in playerteamloader, stadiumloader, matchesloader order
16. Now you can run queries in the SQL session file using -- @block (your query)

# Datasets: 

Matches: https://github.com/schochastics/football-data/blob/master/data/results/games.parquet

Stadiums: https://www.kaggle.com/datasets/imtkaggleteam/football-stadiums?resource=download

Players/Teams: https://www.kaggle.com/datasets/antoinekrajnc/soccer-players-statistics
