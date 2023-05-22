
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub
import os

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

bidsSet, itemsSet, categoriesSet,usersSet = [set() for _ in range(4)]

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == "NULL" or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

def openFiles():
    tables = ["bid.dat", "item.dat", "category.dat","user.dat"]
    return [open(name,'w') for name in tables]
    
def removeDuplicates():
    tables = ["bid.dat", "item.dat", "category.dat","user.dat"]
    for table in tables:
        os.system(f'sort {table} >> temp.dat')
        os.system(f'uniq -u temp.dat {table}')
        os.system("rm temp.dat")

def escape_quotes(s):
    if type(s) is not str: return
    return "\"" + s.replace("\"", "\"\"") + "\""


"""
Parses a single json file. Curre
ntly, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
        for item in items:
            ItemID = item['ItemID']
            Name = escape_quotes(item['Name'])
            Categories = item['Category']
            Currently = transformDollar(item['Currently'])
            Buy_Price = transformDollar(item.get("Buy_Price","NULL"))
            First_Bid = transformDollar(item["First_Bid"])
            Number_of_Bids = item['Number_of_Bids']
            Bids = item.get("Bids")
            SellerLocation = escape_quotes(item["Location"])
            SellerCountry = escape_quotes(item["Country"])
            Started = transformDttm(item["Started"])
            Ends = transformDttm(item["Ends"])
            Seller = item["Seller"]
            SellerUserID = escape_quotes(Seller["UserID"])
            SellerRating = Seller["Rating"]
            Description = escape_quotes(item["Description"] if item["Description"] else "NULL")


            #adding item listing
            itemAttrs = [ItemID, Name, Currently, Buy_Price, First_Bid, Number_of_Bids, Started, Ends, Description, SellerUserID]
            itemsSet.add(columnSeparator.join(itemAttrs)+ "\n")
            
            #adding the seller to users
            userAttrs = [SellerUserID,SellerLocation,SellerCountry,SellerRating]
            usersSet.add(columnSeparator.join(userAttrs)+ "\n")

            #categories
            for category in Categories:
                categoryAttrs = [ItemID, escape_quotes(category)]
                categoriesSet.add(columnSeparator.join(categoryAttrs)+ "\n")

            if Bids != None:
                for bid in Bids:
                    bid = bid["Bid"]
                    bidder = bid['Bidder']
                    bidderID = escape_quotes(bidder["UserID"])
                    bidderRating = bidder["Rating"]
                    bidderLocation = escape_quotes(bidder.get("Location","NULL"))
                    bidderCountry = escape_quotes(bidder.get("Country","NULL"))
                    time = transformDttm(bid["Time"])
                    amount = transformDollar(bid["Amount"])
                    #adding bid
                    bidAttrs = [bidderID,time,amount,ItemID]
                    bidsSet.add(columnSeparator.join(bidAttrs)+ "\n")
                    #adding bidder as user
                    userAttrs = [bidderID,bidderLocation,bidderCountry,bidderRating]
                    usersSet.add(columnSeparator.join(userAttrs)+ "\n")
    

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argumen
    bidsDB, itemsDB, categoriesDB,usersDB = openFiles()
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print("Success parsing " + f)
    bidsDB, itemsDB, categoriesDB,usersDB = openFiles()
    for line in bidsSet:
        bidsDB.write(line)
    for line in itemsSet:
        itemsDB.write(line)
    for line in categoriesSet:
        categoriesDB.write(line)
    for line in usersSet:
        usersDB.write(line) 
    itemsDB.close()
    bidsDB.close()
    categoriesDB.close()
    usersDB.close()
    #removeDuplicates()


if __name__ == '__main__':
    main(sys.argv)
