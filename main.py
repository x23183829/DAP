import pandas as pd
import pymongo
import psycopg2
from sqlalchemy import create_engine


MONGODB_URI = "mongodb://localhost:27017/"
DB_NAME = "DAP"
COLLECTION_NAME = "files"

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
    df.drop(columns=['index_sa','_id','place_id','hpi_type','hpi_flavor'], inplace=True)
    # df.drop_duplicates(inplace=True)
    # df.dropna(inplace=True)
    df = df[df['level'].isin(['USA or Census Division'])]
    df = df[df['frequency'].isin(['monthly'])]
    df.drop(columns=['level', 'frequency'], inplace=True)
    df.rename(columns={'period': 'months','yr': 'year'}, inplace=True)
    return df

def transform_df(df):
    df['index_nsa_euro'] = df['index_nsa'] * 0.94
    df.drop(columns=['index_nsa'], inplace=True)
    y_df = df.groupby('year')['index_nsa_euro'].mean().reset_index()
    m_df = df.groupby('months')['index_nsa_euro'].mean().reset_index()
    return df,y_df, m_df

def add_data_to_postgres(engine, dataframes, table_names):
    # Connect to the database
    conn = engine.connect()

    # Iterate over each DataFrame and add data to the corresponding table
    for table_name, df in zip(table_names, dataframes):
        # Convert DataFrame to SQL table
        df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Close the connection
    conn.close()


def create_postgres_db(host, port, user, password, dbname, dataframe_columns):
    # Connect to PostgreSQL server
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {dbname};")

    # Create the database
    cur.execute(f"CREATE DATABASE {dbname};")

    # Close the cursor and connection
    cur.close()
    conn.close()

    # Connect to the newly created database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

    # Define a function to create tables
    def create_table(connection, table_name, columns):
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {", ".join([f"{col} VARCHAR" for col in columns])}
            );
        """
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)

    # Define table names and columns for each dataframe
    table_names = ['usa_complete', 'yearly_nsa', 'monthly_nsa']
    columns = dataframe_columns

    # Create tables for each dataframe
    with psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname
    ) as conn:
        for table_name, cols in zip(table_names, columns):
            create_table(conn, table_name, cols)

    return engine


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

if __name__ == "__main__":
    df = load_data_from_mongodb(MONGODB_URI, DB_NAME, COLLECTION_NAME)
    # for column_name in df.columns:
    #     unique_values = df[column_name].unique()
    #     print(f"Unique values in column '{column_name}':")
    #     print(unique_values)
    #     print()
    print(df.columns,df.size)
    df = clean_data(df)
    print(df.columns)
    for column_name in df.columns:
        unique_values = df[column_name].unique()
        print(f"Unique values in column '{column_name}':")
        print(unique_values)
        print()
    df, df_y,df_m = transform_df(df) 
    print(df.describe(),"\n",df)
    print(df_y.describe(),"\n",df_y)
    print(df_m.describe(),"\n",df_m.head(13))
    print(df.columns,df_y.columns,df_m.columns)
    engine = create_postgres_db(
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_USER,
        POSTGRES_PASSWORD,
        POSTGRES_DBNAME,
        dataframe_columns
    )

    dataframes = [df, df_y, df_m]
    table_names = ['usa_complete', 'yearly_nsa', 'monthly_nsa']
    add_data_to_postgres(engine, dataframes, table_names)
