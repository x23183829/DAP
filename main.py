import pandas as pd
import pymongo
import json


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

def load_data_from_mongodb(uri, db_name, collection_name):
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    cursor = collection.find({})
    data = list(cursor)
    df = pd.DataFrame(data)
    client.close()
    return df

def clean_data(df):
    df.drop(columns=['index_sa'], inplace=True)
    # df.drop_duplicates(inplace=True)
    # df.dropna(inplace=True)
    df.rename(columns={'period': 'months'}, inplace=True)
    return df


if __name__ == "__main__":
    input_file = "HPI_master.json"
    #load_data_to_mongodb(input_file, MONGODB_URI, DB_NAME, COLLECTION_NAME)
    df = load_data_from_mongodb(MONGODB_URI, DB_NAME, COLLECTION_NAME)
    df = clean_data(df)
    print(df.describe(),"\n",df.head(5))
    

