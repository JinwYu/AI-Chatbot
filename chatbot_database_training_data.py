import sqlite3
import pandas as pd

timeframes = ['2015-01'] #choose one or more database/s, we use the database called 2015-01

for timeframe in timeframes:
    connection = sqlite3.connect('{}.db'.format(timeframe)) #connect to the database
    c = connection.cursor()
    limit = 5000 #how much we are going to pull at 1 time to throw into the panda dataframe
    last_unix = 0 #help us buffer through our database, it will iterate so the next unix has to be higher than the unix before it
    cur_length = limit 
    counter = 0
    test_done = False
    
    while cur_length == limit: #as long as we are able to get the limit from the database
        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} and parent NOT NULL and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,limit),connection) #df stands for dataframe, in pd.read_sql you first pass the sql statement and then you pass the connection
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df) #check the lenght of the dataframe (it should be the same as the unix)
        if not test_done:
            with open("test.from", 'a', encoding='utf8') as f: #open the "from" file
                for content in df['parent'].values:
                    f.write(content+'\n')
            with open("test.to", 'a', encoding='utf8') as f: #open the "to" file
                for content in df['comment'].values:
                    f.write(content+'\n')

            
            test_done = True

        else: #when done and we know that test is successfull, apply it to the training data       
            with open("train.from", 'a', encoding='utf8') as f: #open the "from" file
                for content in df['parent'].values:
                    f.write(content+'\n')
            with open("train.to", 'a', encoding='utf8') as f: #open the "to" file
                for content in df['comment'].values:
                    f.write(content+'\n')


        counter +=1
        #feedback 
        if counter % 20 == 0: #every 100 thousand row completed (because our limit is set to 5000) print the result in the while statement above
            print(counter*limit, 'rows completed so far')
 
#end of program
