# Initialising and Inserting a Single Record to my Postgres DB

import psycopg2 as db

conn_string = "dbname='dataengineering'" \
              "host='localhost'" \
              "user='postgres'" \
              "password='postgres'"

conn = db.connect(conn_string)
cur=conn.cursor()

query = "insert into users (id,name,street,city,zip) values({},'{}','{}','{}','{}')".format(1,'Big Bird','Sesame Street','Fakeville','12345')

## psycopg2.mogrify() can show me the EXACT/FINAL form of query that will be sent to the db.
print(cur.mogrify(query))

## another format of query. lets psycopg2 handle the mapping of types
query2 = "insert into people_information (id,name,street,city,zip) values(%s,%s,%s,%s,%s)"
data=(1,'Big Bird','Sesame Street','Fakeville','12345') # tuple
print(cur.mogrify(query2, data))

## insert that data into the db
cur.execute(query2, data)
conn.commit()


# Inserting Multiple Records
## Create 1000 fake data rows
from faker import Faker

fake = Faker()
data = []
i = 2

for row in range(1000):
    data.append((i, fake.name(), fake.street_address(), fake.city(), fake.zipcode()))
    i += 1

## data feed type: tuple of tuples
data_for_db = tuple(data)
query = "INSERT INTO people_information (id,name,street,city,zip) values(%s,%s,%s,%s,%s)"

cur.executemany(query, data_for_db) #.executemany() for multiple inserts
conn.commit()



# Extracting data from PostgreSQL

query = "SELECT * FROM people_information"
cur.execute(query)

'''
ways of handling returned data from query
1.
for record in cur:
    print(record)
    
2.
cur.fetchall()
cur.fetchmany(25) 25 records
cur.fetchone()


'''
# Assigning to a variable
data = cur.fetchall()
print(data[:10])

## make sure to keep track
print(cur.rowcount) # 1001
print(cur.rownumber)  # which row is next

## writing out to a CSV file
file = open('fromdb.csv', 'w')

cur.copy_to(file, 'people_information', sep=',')
file.close()

## Using Pandas Dataframe
import pandas as pd

df = pd.read_sql("SELECT * FROM people_information", conn)
df.to_json('postgresql_to_json.json', orient='records')

