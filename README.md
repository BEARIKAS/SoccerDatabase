# SoccerDatabase
Soccer Database for soccer application later on

**Steps to run locally** (need python and mysql installed)
1. Have mysql installed, remember your password and username
2. In VScode, install SQLTools and the first driver that comes up when you search SQLTools Driver in extensions
3. Now in your cmd or terminal enter mysql and create a new database "mysql -u root -p", "CREATE DATABASE x;"
4. Name this database soccer if you want to change the code minimally
5. back to VScode go to the SQLTools extension, it should be a cylinder on the lefthand side of your vscode toolbar, then click add new connection
6. Click the mysql option
7. Leave everything as default, but you need a Connection Name which can be whatever, Database should be soccer or whatever you named it, username should be username or root (only root worked for me), change the password option to "save as plaintext" and enter the password below it
8. Click test connection, if it doesn't work it is probably a password or username error, if anything else, maybe the database name
9. click save connection
10. click open connection
11. copy paste the naruraljoin code into your new SoccerDatabase.session
12. Run the Teams, Players, Matches, Stadiums blocks
13. run "pip install kaggle pandas mysql-connector-python" in terminal
14. in each python file there is database connection code called db_config in json format, you probably need to change the password, possibly the username and database name as well
15. Run the Python files in playerteamloader, stadiumloader, matchesloader order
16. Now you can run queries in the SQL session file using -- @block (your query)
