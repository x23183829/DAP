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
    df.drop(columns=['index_sa','_id'], inplace=True)
    # df.drop_duplicates(inplace=True)
    # df.dropna(inplace=True)
    df.rename(columns={'period': 'months','yr': 'year'}, inplace=True)
    return df

def transform_df(df):
    y_df = df.groupby('year')['index_nsa'].mean().reset_index()
    m_df = df.groupby('months')['index_nsa'].mean().reset_index()
    return y_df, m_df


if __name__ == "__main__":
    input_file = "HPI_master.json"
    #load_data_to_mongodb(input_file, MONGODB_URI, DB_NAME, COLLECTION_NAME)
    df = load_data_from_mongodb(MONGODB_URI, DB_NAME, COLLECTION_NAME)
    print(df.columns)
    df = clean_data(df)
    print(df.columns)
    for column_name in df.columns:
        unique_values = df[column_name].unique()
        print(f"Unique values in column '{column_name}':")
        print(unique_values)
        print()
    df_y,df_m = transform_df(df) 
    print(df.describe(),"\n",df.head(5))
    print(df_y.describe(),"\n",df_y.head(5))
    print(df_m.describe(),"\n",df_m.head(13))

    

