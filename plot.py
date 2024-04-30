import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

def load_data_from_postgres(host, port, user, password, dbname, table_names):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    dfs = {}
    for table_name in table_names:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, engine)
        dfs[table_name] = df
    return dfs



def convert_monthly_to_quarterly(usa_monthly_nsa_df):
    usa_monthly_nsa_df['months'] = pd.to_datetime(usa_monthly_nsa_df['months'], format='%m').dt.month_name()
    usa_monthly_nsa_df['Quarter'] = pd.PeriodIndex(usa_monthly_nsa_df['months'], freq='Q').strftime('%YQ%q')
    quarterly_changes = usa_monthly_nsa_df.groupby('Quarter')['index_nsa_euro'].mean()
    return quarterly_changes


def plot_ireland_avg_quarterly_changes(ireland_avg_quarterly_df):
    new_properties = ireland_avg_quarterly_df[ireland_avg_quarterly_df['property_type'] == 'New']
    second_hand_properties = ireland_avg_quarterly_df[ireland_avg_quarterly_df['property_type'] == 'Second Hand']

    plt.figure(figsize=(10, 6))
    plt.plot(new_properties['quarter'], new_properties['average_value'], marker='o', linestyle='-', label='New')
    plt.title('Ireland Quarterly Changes for New Properties')
    plt.xlabel('Quarter')
    plt.ylabel('Average Value')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.savefig('ireland_new_properties_quarterly_changes.png')
    plt.show()

    plt.figure(figsize=(10, 6))
    plt.plot(second_hand_properties['quarter'], second_hand_properties['average_value'], marker='o', linestyle='-', color='orange', label='Second Hand')
    plt.title('Ireland Quarterly Changes for Second Hand Properties')
    plt.xlabel('Quarter')
    plt.ylabel('Average Value')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.savefig('ireland_second_hand_properties_quarterly_changes.png')
    plt.show()

    plt.figure(figsize=(10, 6))
    plt.plot(new_properties['quarter'], new_properties['average_value'], marker='o', linestyle='-', label='New')
    plt.plot(second_hand_properties['quarter'], second_hand_properties['average_value'], marker='o', linestyle='-', color='orange', label='Second Hand')
    plt.title('Ireland Quarterly Changes for Properties')
    plt.xlabel('Quarter')
    plt.ylabel('Average Value')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.savefig('ireland_properties_quarterly_changes.png')
    plt.show()


def plot_ireland_yearly_changes(ireland_avg_yearly_df):
    new_properties = ireland_avg_yearly_df[ireland_avg_yearly_df['property_type'] == 'New']
    second_hand_properties = ireland_avg_yearly_df[ireland_avg_yearly_df['property_type'] == 'Second Hand']

    plt.figure(figsize=(10, 6))
    plt.plot(new_properties['year'], new_properties['average_value'], marker='o', linestyle='-', label='New')
    plt.title('Ireland Yearly Changes for New Properties')
    plt.xlabel('Year')
    plt.ylabel('Average Value')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.savefig('ireland_new_properties_yearly_changes.png')
    plt.show()

    plt.figure(figsize=(10, 6))
    plt.plot(second_hand_properties['year'], second_hand_properties['average_value'], marker='o', linestyle='-', color='orange', label='Second Hand')
    plt.title('Ireland Yearly Changes for Second Hand Properties')
    plt.xlabel('Year')
    plt.ylabel('Average Value')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.savefig('ireland_second_hand_properties_yearly_changes.png')
    plt.show()

    plt.figure(figsize=(10, 6))
    plt.plot(new_properties['year'], new_properties['average_value'], marker='o', linestyle='-', label='New')
    plt.plot(second_hand_properties['year'], second_hand_properties['average_value'], marker='o', linestyle='-', color='orange', label='Second Hand')
    plt.title('Ireland Yearly Changes for Properties')
    plt.xlabel('Year')
    plt.ylabel('Average Value')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.savefig('ireland_properties_yearly_changes.png')
    plt.show()

def plot_usa_yearly_changes(usa_yearly_nsa_df):
    plt.figure(figsize=(10, 6))
    plt.plot(usa_yearly_nsa_df['year'], usa_yearly_nsa_df['index_nsa_euro'], marker='o', linestyle='-')
    plt.title('USA Yearly Changes')
    plt.xlabel('Year')
    plt.ylabel('Index NSA (Euro)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.savefig('usa_yearly_changes.png')
    plt.show()

def plot_yearly_changes_comparison(ireland_avg_yearly_df, usa_yearly_nsa_df):
    new_properties = ireland_avg_yearly_df[ireland_avg_yearly_df['property_type'] == 'New']
    second_hand_properties = ireland_avg_yearly_df[ireland_avg_yearly_df['property_type'] == 'Second Hand']

    plt.figure(figsize=(10, 6))
    plt.plot(new_properties['year'], new_properties['average_value'], marker='o', linestyle='-', label='Ireland New')
    plt.plot(second_hand_properties['year'], second_hand_properties['average_value'], marker='o', linestyle='-', color='green', label='Ireland Second Hand')
    plt.plot(usa_yearly_nsa_df['year'], usa_yearly_nsa_df['index_nsa_euro'], marker='o', linestyle='-', color='orange', label='USA')
    plt.title('Yearly Changes Comparison')
    plt.xlabel('Year')
    plt.ylabel('Average Value (Euro)')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.savefig('yearly_changes_comparison.png')
    plt.show()


def plot_usa_monthly_changes(usa_monthly_nsa_df):
    plt.figure(figsize=(10, 6))
    plt.plot(usa_monthly_nsa_df['months'], usa_monthly_nsa_df['index_nsa_euro'], marker='o', linestyle='-', color='orange')
    plt.title('USA Monthly Changes')
    plt.xlabel('Month')
    plt.ylabel('Average Value (Euro)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.savefig('usa_monthly_changes.png')
    plt.show()







if __name__ == "__main__":
    ireland_table_names = ['Ire_housing', 'avg_quarterly', 'avg_yearly']
    usa_table_names = ['usa_complete', 'yearly_nsa', 'monthly_nsa']
    

    ireland_data = load_data_from_postgres('localhost', '5432', 'postgres', 'abcde', 'ireland_pricing', ireland_table_names)


    usa_data = load_data_from_postgres('localhost', '5432', 'postgres', 'abcde', 'usa_nsa_index', usa_table_names)


    ireland_housing_df = ireland_data['Ire_housing']
    ireland_avg_quarterly_df = ireland_data['avg_quarterly']
    ireland_avg_yearly_df = ireland_data['avg_yearly']

    usa_complete_df = usa_data['usa_complete']
    usa_yearly_nsa_df = usa_data['yearly_nsa']
    usa_monthly_nsa_df = usa_data['monthly_nsa']

    print("Ireland Housing Data:")
    print(ireland_housing_df)

    print("\nIreland Average Quarterly Data:")
    print(ireland_avg_quarterly_df)

    print("\nIreland Average Yearly Data:")
    print(ireland_avg_yearly_df)

    print("\nUSA Complete Data:")
    print(usa_complete_df)

    print("\nUSA Yearly NSA Data:")
    print(usa_yearly_nsa_df)

    print("\nUSA Monthly NSA Data:")
    print(usa_monthly_nsa_df)


    print("Ireland Housing DataFrame columns:", ireland_housing_df.columns,
      "\nUSA Complete DataFrame columns:", usa_complete_df.columns,
      "\nYearly NSA DataFrame columns:", usa_yearly_nsa_df.columns,
      "\nMonthly NSA DataFrame columns:", usa_monthly_nsa_df.columns,
      "\nAverage Quarterly DataFrame columns:", ireland_avg_quarterly_df.columns,
      "\nAverage Yearly DataFrame columns:", ireland_avg_yearly_df.columns)
    


    plot_ireland_avg_quarterly_changes(ireland_avg_quarterly_df)

    plot_ireland_yearly_changes(ireland_avg_yearly_df)

    plot_usa_yearly_changes(usa_yearly_nsa_df)

    plot_yearly_changes_comparison(ireland_avg_yearly_df, usa_yearly_nsa_df)

    plot_usa_monthly_changes(usa_monthly_nsa_df)
