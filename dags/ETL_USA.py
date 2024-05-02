from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pymongo
import psycopg2
from sqlalchemy import create_engine

MONGODB_URI = "mongodb://localhost:27017/"
DB_NAME = "DAP"
COLLECTION_NAME = "files"

POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'abcde'
POSTGRES_DBNAME = 'usa_nsa_index'

dataframe_columns = [
    ['place_name', 'year', 'months', 'index_nsa_euro'],
    ['year', 'index_nsa_euro'],
    ['months', 'index_nsa_euro']
]

def load_data_from_mongodb():
    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    cursor = collection.find({})
    data = list(cursor)
    for item in data:
        if '_id' in item:
            item['_id'] = str(item['_id'])
    df = pd.DataFrame(data)
    print(df)
    client.close()
    return df.to_dict(orient='records')

def clean_data(**context):
    df_dict = context['task_instance'].xcom_pull(task_ids='load_data_from_mongodb')
    df = pd.DataFrame(df_dict)
    print(df)
    df.drop(columns=['index_sa','_id','place_id','hpi_type','hpi_flavor'], inplace=True)
    df = df[df['level'].isin(['USA or Census Division'])]
    df = df[df['frequency'].isin(['monthly'])]
    df.drop(columns=['level', 'frequency'], inplace=True)
    df.rename(columns={'period': 'months','yr': 'year'}, inplace=True)
    return df.to_dict(orient='records')

def transform_df(**context):
    df_dict = context['task_instance'].xcom_pull(task_ids='clean_data')
    df = pd.DataFrame(df_dict)
    df['index_nsa_euro'] = df['index_nsa'] * 0.94
    df.drop(columns=['index_nsa'], inplace=True)
    y_df = df.groupby('year')['index_nsa_euro'].mean().reset_index()
    m_df = df.groupby('months')['index_nsa_euro'].mean().reset_index()
    print(df,y_df,m_df,len(df))
    return { 'y_df': y_df.to_dict(), 'm_df': m_df.to_dict(), 'df': df.to_dict()}

def add_data_to_postgres(**context):
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DBNAME}')
    conn = engine.connect()
    data = context['task_instance'].xcom_pull(task_ids='transform_data')
    print("loading data")
    for table_name, df_dict in zip(['usa_complete', 'yearly_nsa', 'monthly_nsa'], [data['df'], data['y_df'], data['m_df']]):
        print(pd.DataFrame(df_dict))
        pd.DataFrame(df_dict).to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def create_tables():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {POSTGRES_DBNAME};")
    cur.execute(f"CREATE DATABASE {POSTGRES_DBNAME};")
    cur.close()
    conn.close()

    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DBNAME
    ) as conn:
        for table_name, cols in zip(['usa_complete', 'yearly_nsa', 'monthly_nsa'], dataframe_columns):
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {", ".join([f"{col} VARCHAR" for col in cols])}
                );
            """
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)

# Define the DAG
dag = DAG(
    'usa_data_pipeline',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='@daily',
    catchup=False,
)

# Define the tasks
load_data_task = PythonOperator(
    task_id='load_data_from_mongodb',
    python_callable=load_data_from_mongodb,
    dag=dag,
)


clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_df,
    provide_context=True,
    dag=dag,
)

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

add_data_to_postgres_task = PythonOperator(
    task_id='add_data_to_postgres',
    python_callable=add_data_to_postgres,
    provide_context=True,
    dag=dag,
)


load_data_task >> clean_data_task >> transform_data_task >> add_data_to_postgres_task
create_tables_task >> transform_data_task