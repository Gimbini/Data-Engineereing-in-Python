# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


# See PyCharm help at https://www.jetbrains.com/help/pycharm/

import csv
from faker import Faker

# Generating fake data with Faker

with open('data.CSV', 'w') as output:
    header = ['name', 'age', 'street', 'city', 'state', 'zip', 'lng', 'lat']
    fake = Faker()

    my_writer = csv.writer(output)
    my_writer.writerow(header)

    for row in range(1000):
        my_writer.writerow([fake.name(), fake.random_int(min=18, max=80, step=1), fake.street_address(), fake.city(),
                            fake.state(), fake.zipcode(), fake.longitude(), fake.longitude(), fake.latitude()])

# Reading file with csv.DictReader()

with open('data.CSV') as f:
    my_reader = csv.DictReader(f)
    headers = next(my_reader)

''' Check if the file has read successfully

    for row in my_reader:
        print(row['name'])
'''

# Read/Write with Pandas

## Reading file with Pandas
import pandas as pd

df = pd.read_csv('data.CSV')
print(df.head(10))  # first 10 rows

## Creating Pandas Dataframe

data = {'Name':['Bin', 'Yorke', 'Bellamy', 'Frusciante'], 'Age':[25,55,43,54]}
df = pd.DataFrame(data)
print(df)

## Export Pandas Dataframe to CSV
df.to_csv('Musicians.CSV', index=False)


# Read/Write JSON with CSV
## Writing
import json

with open('data.JSON', 'w') as output:
    fake=Faker()
    alldata = {'records':[]}

    for i in range(1000):
        data = {"name":fake.name(), "age":fake.random_int(min=18, max=80, step=1), "street":fake.street_address(),
                "city":fake.city(), "state":fake.state(), "zip":fake.zipcode(), "lng":float(fake.longitude()),
                "lat":float(fake.latitude())}
        alldata['records'].append(data)

    json.dump(alldata, output)

## Reading
with open("data.JSON", 'r') as f:
    data = json.load(f)
    print("Reading JSON File - Test:", data['records'][:5])

# Read/Write JSON with DataFrames
## Read
import pandas.io.json as pd_JSON
with open('data.JSON', 'r') as f:
    data = pd_JSON.loads(f.read()) # First, load the JSON as is
df = pd_JSON.json_normalize(data, record_path='records') # specify the exact location of the data within dictionary
print("JSON to Pands DF - Test: \n", df.head(5))
## Write
print("DF to JSON: Standard \n", df.head(2).to_json())
print("DF to JSON: Orient = 'records' \n", df.head(2).to_json(orient='records')) # emphasis on the orient parameter
    # Author Paul Crickard thinks orient='records' is the better format for later engineering processes

