
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
def preprocess_df(df):
    # Check if 'Year/Qrt' column exists in the DataFrame
    if 'Year/Qrt' in df.columns:
        print('df.columns')
        df['Quarter'] = pd.PeriodIndex(df['Year/Qrt'], freq='Q').strftime('%YQ%q')
        df['Quarter'] = df['Quarter'].str.split('Q').str[1].astype(int)
        df = df.melt(id_vars=['Year/Qrt', 'Quarter'], var_name='City', value_name='Value')
    elif 'Year/Qtr' in df.columns:
        df['Quarter'] = pd.PeriodIndex(df['Year/Qtr'], freq='Q').strftime('%YQ%q')
        df['Quarter'] = df['Quarter'].str.split('Q').str[1].astype(int)
        df = df.melt(id_vars=['Year/Qtr', 'Quarter'], var_name='City', value_name='Value')
    else:
        df['Quarter'] = ''
        df = df.melt(id_vars=['Quarter'], var_name='City', value_name='Value')
    return df


#insert_excel_file('105386_55b2433a-3b8a-4c6c-a1fe-c4d324512ad3.xlsx')
#insert_excel_file('105388_e6ff5cde-36fc-4162-9991-c6041bcb62a6.xlsx')

df2 = load_excel_file('105386_55b2433a-3b8a-4c6c-a1fe-c4d324512ad3.xlsx')
df1 = load_excel_file('105388_e6ff5cde-36fc-4162-9991-c6041bcb62a6.xlsx')


client.close()


print("DataFrame 1:")
print(df1.columns,df1)
print("\nDataFrame 2:")
print(df2.columns)


df1 = preprocess_df(df1)
df2 = preprocess_df(df2)

print("DataFrame 1:")
print(df1['Year/Qrt'])
print("\nDataFrame 2:")
print(df2.columns,df2)

