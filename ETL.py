
import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
import gridfs
import psycopg2
from psycopg2 import sql

client = MongoClient('mongodb://localhost:27017/')
db = client['dap_db']
fs = gridfs.GridFS(db)


dbname = "ireland_pricing"
user = "postgres"
password = "abcde"
host = "localhost"
port = "5432"

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
    if 'Year' in df.columns:
        df['Quarter'] = pd.PeriodIndex(df['Year'], freq='Q').strftime('%YQ%q')
        df['Quarter'] = df['Quarter'].str.split('Q').str[1].astype(int)
        df = df.melt(id_vars=['Year', 'Quarter'], var_name='City', value_name='Value')
        df['Year'] = df['Year'].str.split("Q").str[0]
    return df


def merge_dataframes(df1, df2):
    df1['Property Type'] = 'New'
    df2['Property Type'] = 'Second Hand'
    merged_df = pd.concat([df1, df2], ignore_index=True)
    
    return merged_df



def calculate_average(df):
    average_quarterly = df.groupby(['Property Type', 'Quarter'])['Value'].mean().reset_index()
    average_yearly = df.groupby(['Property Type', 'Year'])['Value'].mean().reset_index()
    
    return average_quarterly, average_yearly


def create_database_tables(dbname, user, password, host, port):
    conn = psycopg2.connect(
        dbname="postgres",
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.autocommit = True

    cur = conn.cursor()
    cur.execute("CREATE DATABASE %s;" % dbname)
    cur.close()
    conn.close()

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )


    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE Ire_housing (
            id SERIAL PRIMARY KEY,
            Year INTEGER,
            Quarter INTEGER,
            City VARCHAR(255),
            Value NUMERIC,
            Property_Type VARCHAR(50)
        );
    """)

    cur.execute("""
        CREATE TABLE avg_quarterly (
            id SERIAL PRIMARY KEY,
            quarter INTEGER,
            property_type VARCHAR(50),
            average_value NUMERIC
        );
    """)

    cur.execute("""
        CREATE TABLE avg_yearly (
            id SERIAL PRIMARY KEY,
            year INTEGER,
            property_type VARCHAR(50),
            average_value NUMERIC
        );
    """)

    conn.commit()
    cur.close()
    conn.close()


def insert_data_to_tables(df_merged, avg_quarterly, avg_yearly, dbname, user, password, host, port):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cur = conn.cursor()
    for index, row in df_merged.iterrows():
        cur.execute(
            sql.SQL("INSERT INTO Ire_housing (Year, Quarter, City, Value, Property_Type) VALUES (%s, %s, %s, %s, %s);"),
            (row['Year'], row['Quarter'], row['City'], row['Value'], row['Property Type'])
        )
    conn.commit()

    for index, row in avg_quarterly.iterrows():
        cur.execute(
            sql.SQL("INSERT INTO avg_quarterly (quarter, property_type, average_value) VALUES (%s, %s, %s);"),
            (row['Quarter'], row['Property Type'], row['Value'])
        )
    conn.commit()
    
    for index, row in avg_yearly.iterrows():
        cur.execute(
            sql.SQL("INSERT INTO avg_yearly (year, property_type, average_value) VALUES (%s, %s, %s);"),
            (row['Year'], row['Property Type'], row['Value'])
        )
    conn.commit()
    
    cur.close()
    conn.close()

#insert_excel_file('105386_55b2433a-3b8a-4c6c-a1fe-c4d324512ad3.xlsx')
#insert_excel_file('105388_e6ff5cde-36fc-4162-9991-c6041bcb62a6.xlsx')

df1 = load_excel_file('105386_55b2433a-3b8a-4c6c-a1fe-c4d324512ad3.xlsx')
df2 = load_excel_file('105388_e6ff5cde-36fc-4162-9991-c6041bcb62a6.xlsx')


client.close()


print("DataFrame 1:")
print(df1.columns)
print("\nDataFrame 2:")
print(df2.columns)

df1.rename(columns={'Year/Qtr': 'Year'}, inplace=True)
df2.rename(columns={'Year/Qrt': 'Year'}, inplace=True)

df1 = preprocess_df(df1)
df2 = preprocess_df(df2)

print("DataFrame 1:")
print(df1)
print("\nDataFrame 2:")
print(df2)

df_new = merge_dataframes(df1, df2)
print(df_new)

average_quarterly, average_yearly = calculate_average(df_new)

print("Average value for each quarter:")
print(average_quarterly)
print("\nAverage value for each year:")
print(average_yearly)


print(df_new.columns, average_quarterly.columns, average_yearly.columns)

#create_database_tables(dbname, user, password, host, port)

insert_data_to_tables(df_new, average_quarterly, average_yearly,dbname, user, password, host, port)