import pandas as pd
from pymongo import MongoClient
import gridfs


client = MongoClient('mongodb://localhost:27017/')
db = client['dap_db'] 
fs = gridfs.GridFS(db)

def insert_excel_file(filename):
    with open(filename, 'rb') as f:
        fs.put(f, filename=filename)

insert_excel_file('105386_55b2433a-3b8a-4c6c-a1fe-c4d324512ad3.xlsx')
insert_excel_file('105388_e6ff5cde-36fc-4162-9991-c6041bcb62a6.xlsx')

client.close()