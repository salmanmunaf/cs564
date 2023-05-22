DROP TABLE IF EXISTS Item;
DROP TABLE IF EXISTS Bid;
DROP TABLE IF EXISTS User;
DROP TABLE IF EXISTS Category;

CREATE TABLE Item (
    ItemID INT NOT NULL UNIQUE,
    Name TEXT NOT NULL,
    Currently FLOAT NOT NULL,
    Buy_Price FLOAT,
    First_Bid FLOAT NOT NULL,
    Number_of_Bids INT NOT NULL,
    Started DATETIME NOT NULL,
    Ends DATETIME NOT NULL,
    Description TEXT NOT NULL,
    UserID INT NOT NULL,
    PRIMARY KEY (ItemID),
    FOREIGN KEY (UserID) REFERENCES User (UserID)
);

CREATE TABLE Bid (
    UserID INT NOT NULL,
    Time DATETIME NOT NULL,
    Amount FLOAT NOT NULL,
    ItemID INT NOT NULL,
    PRIMARY KEY (ItemID,Amount,Time),
    FOREIGN KEY (UserID) REFERENCES User (UserID),
    FOREIGN KEY (ItemID) REFERENCES Item (ItemID)
);

CREATE TABLE User (
    UserID INT UNIQUE NOT NULL,
    Location TEXT,
    Country TEXT,
    Rating INT NOT NULL,
    PRIMARY KEY (UserID)
);

CREATE TABLE Category (
    ItemID INT NOT NULL,
    Category TEXT NOT NULL,
    PRIMARY KEY (ItemID,Category),
    FOREIGN KEY (ItemID) REFERENCES Item (ItemID)
);





