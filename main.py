import pandas as pd
import pymongo
import json

def load_data_to_mongodb(input_file):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["DAP"]
    with open(input_file, 'r') as file:
        json_data = json.load(file)
    collection = db["files"]
    for doc in json_data:
        collection.insert_one(doc)
    client.close()

if __name__ == "__main__":
    input_file = "HPI_master.json"
    load_data_to_mongodb(input_file)
