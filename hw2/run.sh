rm hw2.db
python skeleton_parser.py ebay_data/items-*.json 
sqlite3 hw2.db < create.sql 
sqlite3 hw2.db < load.txt 
sqlite3 hw2.db < query1.sql 
sqlite3 hw2.db < query2.sql 
sqlite3 hw2.db < query3.sql 
sqlite3 hw2.db < query4.sql 
sqlite3 hw2.db < query5.sql 
sqlite3 hw2.db < query6.sql 
sqlite3 hw2.db < query7.sql 
