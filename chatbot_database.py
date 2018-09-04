import sqlite3
import json
from datatime import datatime

timeframe = '2015-05'
sql_transaction = []

# Create database.
connection = sqlite3.connect('{}2.db'.format(timeframe))
c = connection.cursor()

# Only create table if it doesn't exist.
def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply
              (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT,
              comment TEXT, subreddit TEXT, unix INT, score INT)""")

# Create table if it doesn't exist.

if __name__ == '__main__':
    create_table()
