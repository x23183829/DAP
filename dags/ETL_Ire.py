from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
import gridfs
import psycopg2
from psycopg2 import sql


def load_excel_file(filename):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['dap_db']
    fs = gridfs.GridFS(db)
    file_record = db.fs.files.find_one({'filename': filename})
    if file_record:
        file_content = fs.get(file_record['_id']).read()
        df = pd.read_excel(file_content,  header=1 ,skipfooter=4,engine='openpyxl')
        client.close()
        return df
    else:
        client.close()
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
    merged_df['Value'] = merged_df['Value'] / 650
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
    print("Creating DB and Tables")
    conn.autocommit = True
    
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {dbname};")
    
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



def insert_data_to_PostgreSQL(**kwargs):
    ti = kwargs['ti']
    xcom_data = ti.xcom_pull(task_ids='load_and_process')
    df_new = pd.DataFrame(xcom_data['df_new'])
    avg_quarterly = pd.DataFrame(xcom_data['avg_quarterly'])
    avg_yearly = pd.DataFrame(xcom_data['avg_yearly'])
    insert_data(df_new, avg_quarterly, avg_yearly, "ireland_pricing", "postgres", "abcde", "localhost", "5432")

def insert_data(df_merged, avg_quarterly, avg_yearly, dbname, user, password, host, port):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    print(df_merged, avg_quarterly, avg_yearly)
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


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'ireland_housing_pipeline',
    default_args=default_args,
    description='A DAG to process Ireland housing data',
    schedule='@daily',
)


def load_and_process():
    df1 = load_excel_file('105386_55b2433a-3b8a-4c6c-a1fe-c4d324512ad3.xlsx')
    df2 = load_excel_file('105388_e6ff5cde-36fc-4162-9991-c6041bcb62a6.xlsx')
    df1.rename(columns={'Year/Qtr': 'Year'}, inplace=True)
    df2.rename(columns={'Year/Qrt': 'Year'}, inplace=True)
    df1 = preprocess_df(df1)
    df2 = preprocess_df(df2)
    print(df1,df2)
    df_new = merge_dataframes(df1, df2)
    print(df_new)
    average_quarterly, average_yearly = calculate_average(df_new)
    return {'df_new': df_new.to_dict(), 'avg_quarterly': average_quarterly.to_dict(), 'avg_yearly': average_yearly.to_dict()}



load_and_process_task = PythonOperator(
    task_id='load_and_process',
    python_callable=load_and_process,
    provide_context=True,
    dag=dag,
)

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_database_tables,
    op_kwargs={
        'dbname': "ireland_pricing",
        'user': "postgres",
        'password': "abcde",
        'host': "localhost",
        'port': "5432"
    },
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data_to_PostgreSQL,
    provide_context=True,
    dag=dag,
)


load_and_process_task >> create_tables_task >> insert_data_task
