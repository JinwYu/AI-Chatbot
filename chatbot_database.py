import sqlite3
import json
from datetime import datetime
import time

timeframe = '2015-01'
sql_transaction = []
start_row = 0
cleanup = 1000000

# Create database.
connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()


# Only create table if it doesn't exist.
def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply
              (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT,
              comment TEXT, subreddit TEXT, unix INT, score INT)""")


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
        #print(str(e))
        return False #return false if anything goes wrong
    

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
    if len(data.split(' ')) > 1000 or len(data) < 1:
        return False
    elif len(data) > 32000:
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True


#takes in sql statement and keeps building until it reaches a certain size
def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000: #make it faster to read the database by splitting
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction: #for each of the sql statement
            try:
                c.execute(s)
            except:
                pass #holy sin
        connection.commit()
        sql_transaction = [] #empty it out

#"Just copy and paste: I see no idea of why we should write all these queries (programming language to retrieve information from a database) manually"
        
    #basically overwrite the information in 'sql = ' by using the parent_id (any reply to the parent comment we want to make sure that's the new comment)
def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql) #transaction_bldr needs to be defined
    except Exception as e:
        print('s-UPDATE insertion',str(e))

    #basically we insert a new row at the position we have a parentid but we also have the data for that parent (we inserting the information about the parent body)
def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-PARENT insertion',str(e))

    #we are inserting there was no parent but we want to have the parentid just in case somehow it was out of order but also mainly
    #we are inserting this one so we have parent information for another comment whose parent might be this comment
def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-NO_PARENT insertion',str(e))

    
# Create table if it doesn't exist.
if __name__ == '__main__':
    create_table()


#------------------------We want to buffer through the data-------------------------------

    row_counter = 0 #feedback: how far we come when buffering the data 
    paired_rows = 0 #feedback: how many rows of data that we have paired
    
# The address to the database that stores the comments:
# byt "/" till "\\" och skapa en map som kallas för 2015 där filerna "RC_2015-01" ska ligga i

# make use of buffering parameters because the file are too large - work with chunks    
    with open ("C:\\Users\\Martin\\Desktop\\AI_KURS\\Reddit_comment_database\\{}\\RC_{}".format(timeframe.split('-')[0], timeframe), buffering=1000) as f:
        for row in f: 
            #print(row) #feedback            
            row_counter += 1

            if row_counter > start_row:
                try:
                    row = json.loads(row) #read data with python object => reurn a string in json format
                    parent_id = row['parent_id']
                    body = format_data(row['body']) #function need to be defined
                    created_utc = row['created_utc']
                    score = row['score']
                    
                    comment_id = row['name']
                    
                    subreddit = row['subreddit']
                    #not all comments are parent => need to set parent
                    parent_data = find_parent(parent_id) #function need to be defined

# -------------Filter/restrict comments - only use good comments---------------------------

           
                    existing_comment_score = find_existing_score(parent_id) #function need to be defined
                    #if parent comment already has a reply compare with the current comments upvote
                    if existing_comment_score: #if it has any value it will be true
                        if score > existing_comment_score: #if reply has better upvote
                            if acceptable(body): #proceed if the body is acceptable
                                sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)  #the parameters that needs to be passed if we want to replace a comment

                    else:
                        if acceptable(body): #proceed if the body is acceptable
                            if parent_data: #if there is a parent that we have data for
                                if score >= 2: #proceed if the score of the comment is higher than 2
                                    sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score) #the parameters that needs to be passed if we want to replace a comment
                                    paired_rows +=1
                            else:
                                #every comment has a parent_id: if the comment doesn't have a parent it becomes the thread itself hence it becomes a parent
                                sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score) #the parameters that needs to be passed if we want to replace a comment
                except Exception as e:
                    print(str(e))

            #feedback
            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))

            #delete bloat (remove empty comments that didnt make it)
            if row_counter > start_row:
                if row_counter % cleanup == 0:
                    print("Cleanin up!")
                    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                    c.execute(sql)
                    connection.commit()
                    c.execute("VACUUM")
                    connection.commit()
