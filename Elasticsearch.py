from elasticsearch import Elasticsearch
from faker import Faker

fake = Faker()

# Connection and Data Insertion

## Creating Connection
es = Elasticsearch()

## ES instance consists of index, doc_type, and body (JSON object). Like key:value ~ index:body pair
doc = {"name": "Bin Kim", "street": fake.street_address(), "city": fake.city(), "zip": fake.zipcode()}
insert = es.index(index="users", doc_type="doc", body=doc)
print(insert['result'])

## Inserting data in bulk
from elasticsearch import helpers

actions = [
    {
        "_index": "users",
        "_type": "doc",
        "_source": {
            "name": fake.name(),
            "street": fake.street_address(),
            "city": fake.city(),
            "zip": fake.zipcode()}

    }
    for x in range(999)  # or for i,r in df.iterrows() for pandas data frames
]

insert_bulk = helpers.bulk(es, actions)
print(insert_bulk)

# Querying Elasticsearch
## Query All
doc = {"query": {"match_all": {}}}

query_result = es.search(index="users", body=doc, size=10)
print(query_result['hits']['hits'])

## Loading the queried JSON file to dataframe

from pandas import json_normalize

df = json_normalize(query_result['hits']['hits'])

## Query - match
doc = {"query": {"match": {"name": "Bin Kim"}}}
match_result = es.search(index="users", body=doc, size=10)
print(match_result['hits']['hits'][0]['_source'])

## Query using Lucene syntax
lucene_result = es.search(index="users", q="name:Bin Kim, size=10")
print(lucene_result['hits']['hits'][0]['_source'])

## Boolean queries for multiple search criteria
### Use must, must not, should and etc.
doc = {"query": {
    "bool": {
        "must": {
            "match": {
                "name": "Kim"}},
        "filter": {
            "term": {
                "city": "Wellington"}
        }}}}
    # This query matches records with people that have "Kim" in their name, but only those who live in Wellington


## using scroll to handle larger requests - PIT (point in time) is more recommended by the documentation.
scroll_result = es.search(
    index = 'users',
    doc_type = 'doc',
    scroll = '100m',
    size = 950, # 950 missing
    body = {"query":{"match_all":{}}}
)

sid = scroll_result['_scroll_id']
size = scroll_result['hits']['total']['value']

while (size > 0): # start scrolling, like a cursor
    result = es.scroll(scroll_id=sid, scroll='100m')
    sid = result['_scroll_id']
    size = len(result['hits']['hits'])
    for doc in result['hits']['hits']:
        print(doc['_source'])



### reset my es index so I don't stack 1000 records everytime I run this file
es.indices.delete(index='users')
