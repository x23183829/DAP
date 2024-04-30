
import pandas as pd

from pymongo import MongoClient
import gridfs


client = MongoClient('mongodb://localhost:27017/')
db = client['dap_db']
fs = gridfs.GridFS(db)

def insert_excel_file(filename):
    with open(filename, 'rb') as f:
        fs.put(f, filename=filename)


def load_excel_file(filename):
    file_record = db.fs.files.find_one({'filename': filename})
    if file_record:
        file_content = fs.get(file_record['_id']).read()
        df = pd.read_excel(file_content,  header=1 ,skipfooter=4,engine='openpyxl')
        return df
    else:
        print(f"File '{filename}' not found in MongoDB.")




#insert_excel_file('105386_55b2433a-3b8a-4c6c-a1fe-c4d324512ad3.xlsx')
#insert_excel_file('105388_e6ff5cde-36fc-4162-9991-c6041bcb62a6.xlsx')

df1 = load_excel_file('105386_55b2433a-3b8a-4c6c-a1fe-c4d324512ad3.xlsx')
df2 = load_excel_file('105388_e6ff5cde-36fc-4162-9991-c6041bcb62a6.xlsx')


client.close()


print("DataFrame 1:")
print(df1.columns,df1)
print("\nDataFrame 2:")
print(df2.columns)

