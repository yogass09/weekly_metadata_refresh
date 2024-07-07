import pyodbc
import requests
import time
import pandas as pd
from pandas.core.computation.check import NUMEXPR_INSTALLED


###



# Replace with your CoinMarketCap API key
API_KEY = '9d5aff71-7d4b-48f3-9afd-6532c5a1cd69'


# Define connection parameters
server = 'cp-io-sql.database.windows.net'
database = 'sql_db_ohlcv'
username = 'yogass09'
password = 'Qwerty@312'
driver = 'ODBC Driver 17 for SQL Server'
port = 1433

# Define the SQL query to fetch the slug column
query = 'SELECT slug FROM [dbo].[crypto.exchanges.list]'

# Connect to SQL Server and fetch slugs
connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
df_slugs = pd.read_sql(query, connection)
slug_list = df_slugs['slug'].str.lower().tolist()  # Convert slugs to lowercase
connection.close()

# Function to fetch exchange info in batches
def fetch_exchange_info(slug_list, batch_size=100):
    # API endpoint and headers
    url = 'https://pro-api.coinmarketcap.com/v1/exchange/info'
    headers = {'X-CMC_PRO_API_KEY': API_KEY}

    df_list = []
    for i in range(0, len(slug_list), batch_size):
        batch_slugs = slug_list[i:i+batch_size]

        # Filter out any invalid or reserved slugs
        valid_slugs = [slug for slug in batch_slugs if slug not in ['reserved1', 'reserved2']]

        params = {'slug': ','.join(valid_slugs), 'aux': 'urls,logo,description,date_launched,notice,status'}
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()['data']
            df = pd.DataFrame(data).transpose().reset_index(drop=True)
            df_list.append(df)
        else:
            print(f"Failed to fetch data for batch {i+1}-{i+len(valid_slugs)}: {response.text}")
        
        # Sleep for a short interval to respect API rate limits
        time.sleep(60)  # Adjust as needed based on API rate limit
        
    if df_list:
        result_df = pd.concat(df_list, ignore_index=True)
        result_df.to_csv('exchanges_info.csv', index=False)
        print("Data fetched and saved successfully.")
        return result_df
    else:
        print("No data fetched.")

# Example usage
fetch_exchange_info(slug_list)


###


import pyodbc
import pandas as pd

# Define connection parameters
server = 'cp-io-sql.database.windows.net'
database = 'sql_db_ohlcv'
username = 'yogass09'
password = 'Qwerty@312'
driver = 'ODBC Driver 17 for SQL Server'
port = 1433

# Example DataFrame assumed to be fetched already
df_exchange_info = pd.DataFrame({
    'slug': ['exchange1', 'exchange2'],
    'urls': ['https://exchange1.com', 'https://exchange2.com'],
    'logo': ['logo1.png', 'logo2.png'],
    'description': ['Description of exchange1', 'Description of exchange2'],
    'date_launched': ['2022-01-01', '2020-05-15'],
    'notice': ['Notice for exchange1', 'Notice for exchange2'],
    'status': ['Active', 'Inactive']
})

def push_to_sql(dataframe):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(f'DRIVER={driver};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}')
        cursor = conn.cursor()

        # Insert DataFrame records into SQL Server
        for index, row in dataframe.iterrows():
            cursor.execute('''
                INSERT INTO [dbo].[ExchangeInfo] ([slug], [urls], [logo], [description], [date_launched], [notice], [status])
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', row['slug'], row['urls'], row['logo'], row['description'], row['date_launched'], row['notice'], row['status'])
        
        conn.commit()
        print("Data successfully pushed to SQL database.")
    
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    
    finally:
        cursor.close()

# Example usage
push_to_sql(df_exchange_info)



