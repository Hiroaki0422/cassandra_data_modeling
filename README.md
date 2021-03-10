# About this Project
I modeled song and log data of music app for NoSQL database Apache Cassandra. This modeling process will be used for reference or snippet for my future development. 

**Why Using Cassandra?**
Cassandra can replicate data across nodes that makes the data highly-available. The multi-nodes structure is also great for write-heavy workload.

**Consideration Before Using Cassandra**
When you want to run complex analytics, cassandra cannot provide join or aggregation. Relational Database is better for analytical purpose. You want to define primary key, partition key, and composite key well so that the data will be distributed even across the nodes and the unique constraint will not be lost. It is essential for you to know the query beforehand to develop cassandra data model

# Part I. ETL Pipeline for Pre-Processing the Files

## PRE-PROCESSING THE FILES

#### Import Python packages 


```python
# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
```

#### Creating list of filepaths to process original event csv data files


```python
# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)
```

    /home/workspace


#### Processing the files to create the data file csv that will be used for Apache Casssandra tables


```python
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
 
print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
# print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

```

    8056



```python
# check the number of rows in the csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
```

    6821


## Begin writing your Apache Cassandra code in the cells below

#### Creating a Cluster


```python
# This should make a connection to a Cassandra instance in the local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])

# To establish connection and begin executing queries, need a session
session = cluster.connect()
```

#### Create Keyspace


```python
try:
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS mytable
        WITH REPLICATION =
        {'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """)
except Exception as e:
    print(e)
```

#### Set Keyspace


```python
try: 
    session.set_keyspace('mytable')
except Exception as e:
    print(e)
```

#### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

## Create queries to ask the following three questions of the data

1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'




## Query1 


```python
try:
    session.execute("drop table if exists song_info")
except Exception as e:
    print(e)
```


```python
query = """
    CREATE TABLE IF NOT EXISTS song_info
    (sessionId int, iteminSession int, userId int, song_artist text, song_title text, song_length float, \
    PRIMARY KEY (sessionId, iteminSession, userId))
"""

try:
    session.execute(query)
except Exception as e:
    print(e)         
```


```python
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO song_info (sessionId, iteminSession, userId, song_artist, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), int(line[10]), line[0], line[9], float(line[5])))
```

#### Do a SELECT to verify that the data have been inserted into each table


```python
query = "SELECT song_artist, song_title, song_length FROM song_info "
query = query + "WHERE sessionId = 338 AND itemInSession = 4"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)

rows
```




    <cassandra.cluster.ResultSet at 0x7f9607cd8cf8>




```python
for row in rows:
    print(row)
```

    Row(song_artist='Faithless', song_title='Music Matters (Mark Knight Dub)', song_length=495.30731201171875)


## Query 2


```python
try:
    session.execute("drop table if exists song_playlist_session;")
except Exception as e:
    print(e)
```


```python
query = """
    CREATE TABLE IF NOT EXISTS song_playlist_session
    (userid int, sessionid int, itemInSession int, artist text, song_title text, firstName text, lastName text, \
    PRIMARY KEY (userId, sessionId, itemInSession))
"""

try:
    session.execute(query)
except Exception as e:
    print(e)         
```


```python
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO song_playlist_session (userid, sessionid, itemInSession, artist, song_title, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

                    
```


```python
query = "SELECT artist, song_title, firstName, lastName FROM song_playlist_session "
query = query + "WHERE userid = 10 AND sessionid = 182"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row)
```

    Row(artist='Down To The Bone', song_title="Keep On Keepin' On", firstname='Sylvie', lastname='Cruz')
    Row(artist='Three Drives', song_title='Greece 2000', firstname='Sylvie', lastname='Cruz')
    Row(artist='Sebastien Tellier', song_title='Kilometer', firstname='Sylvie', lastname='Cruz')
    Row(artist='Lonnie Gordon', song_title='Catch You Baby (Steve Pitron & Max Sanna Radio Edit)', firstname='Sylvie', lastname='Cruz')


## Query3


```python
# Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
try:
    session.execute("drop table if exists song_user")
except Exception as e:
    print(e)                  
```


```python
query = """
    CREATE TABLE IF NOT EXISTS song_user
    (song_title text, userId int, firstName text, lastName text, \
    PRIMARY KEY (song_title, userId))
"""

try:
    session.execute(query)
except Exception as e:
    print(e)         
```


```python
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO song_user (song_title, userId, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))
```


```python
query = "SELECT firstName, lastName FROM song_user "
query = query + "WHERE song_title = 'All Hands Against His Own'"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row)
```

    Row(firstname='Jacqueline', lastname='Lynch')
    Row(firstname='Tegan', lastname='Levine')
    Row(firstname='Sara', lastname='Johnson')


### Drop the tables before closing out the sessions


```python
try:
    session.execute("drop table if exists song_info")
except Exception as e:
    print(e)
    
try:
    session.execute("drop table if exists user_info")
except Exception as e:
    print(e)
    
try:
    session.execute("drop table if exists user_info")
except Exception as e:
    print(e)

```

### Close the session and cluster connection¶


```python
session.shutdown()
cluster.shutdown()
```


```python

```


```python

```
