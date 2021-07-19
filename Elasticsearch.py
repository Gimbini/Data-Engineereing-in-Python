from elasticsearch import Elasticsearch
from faker import Faker

fake = Faker()

# Connection and Data Insertion

## Creating Connection
es = Elasticsearch()

## ES instance consists of index, doc_type, and body (JSON object). Like key:value ~ index:body pair
doc={"name": fake.name(),"street": fake.street_address(), "city": fake.city(),"zip":fake.zipcode()}
insert = es.index(index="users",doc_type="doc",body=doc)
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
    for x in range(999) # or for i,r in df.iterrows() for pandas data frames
]

insert_bulk = helpers.bulk(es, actions)
print(insert_bulk)

# Querying Elasticsearch

doc = {"query": {"match_all":{}}}

query_result = es.search(index="users", body=doc, size=10)
print(query_result['hits']['hits'])

