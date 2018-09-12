import sqlite3
import json
from datetime import datetime

timeframe = '2015-01'
sql_transaction = []

# Create database.
connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

# Only create table if it doesn't exist.
def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply
              (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT,
              comment TEXT, subreddit TEXT, unix INT, score INT)""")

# -------------------------------Needs to be defined for [part 3]--------------------------- 

def format_data(data): #take in data
    data = data.replace('\n', ' newlinechar ' ).replace('\r', ' newlinechar ' ).replace('"', "'") #get rid of new lines, return and doubleqoute dvs "
    return data #return data after done formating

def find_parent(pid): #set a comment as "parent" with parent_id
    try:
        sql =  "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid) #comment_id becomes the parent
        c.execute(sql)
        result = c.fetchone() #set result as fetchone, no idea what fetchone is
        if result != None:
            return result[0] #selecting one comment
        else: return False
    except Exception as e:
        print(str(e))
        return False #return false if anything goes wrong
    
# -------------------------------Needs to be defined for [part 3]--------------------------

# -------------------------------Needs to be defined for [part 4]--------------------------
def find_existing_score(pid): #find a reply for the parent comment
    try:
        sql =  "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid) #check upvotes dvs "score" med parent_id
        c.execute(sql)
        result = c.fetchone() #set result as fetchone, no idea what fetchone is
        if result != None:
            return result[0] #selecting one comment
        else: return False
    except Exception as e:
        print(str(e))
        return False #return false if anything goes wrong

#avoid long comments, short comments and comments that has been deleted or removed
def acceptable(data): #take in the data which is the comment
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True
# -------------------------------Needs to be defined for [part 4]--------------------------

    
# Create table if it doesn't exist.
if __name__ == '__main__':
    create_table()

# --------------------------Start of [PART 3]---------------------------------------------
#------------------------We want to buffer through the data-------------------------------

    row_counter = 0 #feedback: how far we come when buffering the data 
    paired_rows = 0 #feedback: how many rows of data that we have paired
    
# The address to the database that stores the comments:
# byt "/" till "\\" och skapa en map som kallas för 2015 där filerna "RC_2015-01" ska ligga i
# [Martin] - "C:\\Users\\Martin\\Desktop\\AI_KURS\\Reddit_comment_database\\{}\\RC_{}"  
# [Jinwoo] -

# make use of buffering parameters because the file are too large - work with chunks    
    with open ("C:\\Users\\Martin\\Desktop\\AI_KURS\\Reddit_comment_database\\{}\\RC_{}".format(timeframe.split('-')[0], timeframe), buffering=1000) as f:
        for row in f: #iterate through f
            print(row) #feedback            
            row_counter += 1
            row = json.loads(row) #read data with python object => reurn a string in json format
            parent_id = row['parent_id']
            body = format_data(row['body']) #function need to be defined 
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['name']
            subreddit = row['subreddit']

            #not all comments are parent => need to set parent
            parent_data = find_parent(parent_id) #function need to be defined

# ---------------------------------End of [PART 3]---------------------------------------- 


# ---------------------------------Start of [PART 4]--------------------------------------
# -------------Filter/restrict comments - only use good comments---------------------------

            if score >= 2: #only use comments with 2+ upvotes
                existing_comment_score = find_existing_score(parent_id) #function need to be defined
                #if parent comment already has a reply compare with the current comments upvote
                if existing_comment_score: #if it has any value it will be true
                    if score > existing_comment_score: #if reply has better upvote
                                                
# ---------------------------------Start of [PART 4]--------------------------------------



    
