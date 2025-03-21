

-- @block
CREATE TABLE Users(
    User_ID INT PRIMARY KEY AUTO_INCREMENT,
    User_First_Name VARCHAR(255),
    User_Last_Name VARCHAR(255),
    User_Email VARCHAR(255)
);

-- @block
CREATE TABLE Stadiums(
    Stadium_ID INT PRIMARY KEY AUTO_INCREMENT,
    Stadium_Name VARCHAR(255),
    Stadium_Country VARCHAR(255),
    Stadium_Confederation VARCHAR(255),
    Stadium_City VARCHAR(255)
);

-- @block
CREATE TABLE Teams (
    Team_ID INT PRIMARY KEY,
    Team_Name VARCHAR(255),
    Home_Stadium INT,
    FOREIGN KEY (Home_Stadium) REFERENCES Stadiums(Stadium_ID)
);

-- @block
CREATE TABLE Matches (
    Match_ID INT PRIMARY KEY,
    Match_Date VARCHAR(255),
    Match_Tournament VARCHAR(255),
    Away_Goals INT,
    Home_Goals INT,
    Home_Team INT,
    Away_Team INT,
    FOREIGN KEY (Home_Team) REFERENCES Teams(Team_ID),
    FOREIGN KEY (Away_Team) REFERENCES Teams(Team_ID)
);

-- @block
CREATE TABLE Players (
    Player_ID INT PRIMARY KEY,
    Player_Name VARCHAR(255),
    Team_ID INT,
    Position VARCHAR(255),
    Overall_Rating INT,
    Ball_Control INT,
    Stamina INT,
    Potential INT,
    Short_Passing INT,
    Shot_Power INT,
    Agility INT,
    Penalties INT,
    Free_Kick_Accuracy INT,
    Strength INT,
    FOREIGN KEY (Team_ID) REFERENCES Teams(Team_ID)
);

-- @block
CREATE TABLE Favorite_Teams (
    User_ID INT,
    Team_ID INT,
    PRIMARY KEY (User_ID, Team_ID),
    FOREIGN KEY (User_ID) REFERENCES Users(User_ID),
    FOREIGN KEY (Team_ID) REFERENCES Teams(Team_ID)
);

-- @block
CREATE TABLE Favorite_Players (
    User_ID INT,
    Player_ID INT,
    PRIMARY KEY (User_ID, Player_ID),
    FOREIGN KEY (User_ID) REFERENCES Users(User_ID),
    FOREIGN KEY (Player_ID) REFERENCES Players(Player_ID)
);

