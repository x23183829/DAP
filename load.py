import pandas as pd
from pymongo import MongoClient
import gridfs
import json
import pymongo


client = MongoClient('mongodb://localhost:27017/')
db = client['dap_db']
fs = gridfs.GridFS(db)


MONGODB_URI = "mongodb://localhost:27017/"
DB_NAME = "DAP"
COLLECTION_NAME = "files"

def load_data_to_mongodb(input_file, uri, db_name, collection_name):
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    with open(input_file, 'r') as file:
        json_data = json.load(file)
    collection = db[collection_name]
    for doc in json_data:
        collection.insert_one(doc)
    client.close()

def insert_excel_file(filename):
    with open(filename, 'rb') as f:
        fs.put(f, filename=filename)



insert_excel_file('105386_55b2433a-3b8a-4c6c-a1fe-c4d324512ad3.xlsx')
insert_excel_file('105388_e6ff5cde-36fc-4162-9991-c6041bcb62a6.xlsx')
print("Excel files Loaded")

input_file = "HPI_master.json"

load_data_to_mongodb(input_file, MONGODB_URI, DB_NAME, COLLECTION_NAME)
print("Json files Loaded")