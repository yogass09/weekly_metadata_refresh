import pyodbc

# Define connection parameters
server = 'cp-io-sql.database.windows.net'
database = 'sql_db_ohlcv'
username = 'yogass09'
password = 'Qwerty@312'
driver = 'ODBC Driver 17 for SQL Server'
port = 1433

# Establish the connection
conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
)

# Create a cursor object
cursor = conn.cursor()

# Define the SQL query to fetch the slug column
query = 'SELECT slug FROM [dbo].[crypto.list]'

# Execute the query
cursor.execute(query)

# Fetch all results
slugs = cursor.fetchall()

# Process the results to create a list of slugs
slug_list = [slug[0] for slug in slugs]

# Create a comma-separated string of slugs
slug_csv = ','.join(slug_list)

# Print the comma-separated list of slugs
print(slug_csv)

# Close the cursor and connection
cursor.close()
conn.close()


###


#This example uses Python 2.7 and the python-request library.

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pyodbc
import requests
import time
import pandas as pd


###



# Define connection parameters
server = 'cp-io-sql.database.windows.net'
database = 'sql_db_ohlcv'
username = 'yogass09'
password = 'Qwerty@312'
driver = 'ODBC Driver 17 for SQL Server'
port = 1433

# Establish the connection
conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
)

# Create a cursor object
cursor = conn.cursor()



###



# Define the SQL query to fetch the slug column
query = 'SELECT slug FROM [dbo].[crypto.list]'

# Execute the query
cursor.execute(query)

# Fetch all results
slugs = cursor.fetchall()

# Process the results to create a list of slugs
slug_list = [slug[0] for slug in slugs]

# Close the cursor and connection
cursor.close()
conn.close()

# Replace with your CoinMarketCap API key
API_KEY = '9d5aff71-7d4b-48f3-9afd-6532c5a1cd69'

# API endpoint
url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/info'

# Prepare headers with API key
headers = {
    'X-CMC_PRO_API_KEY': API_KEY
}

# Function to make API requests in batches
def fetch_cryptocurrency_info(slug_batch):
    params = {
        'slug': ','.join(slug_batch),
        'aux': 'urls,logo,description'  # Adjust auxiliary fields as needed
    }
    response = requests.get(url, headers=headers, params=params)
    return response

# Batch size
batch_size = 199  # Adjust the batch size as needed
total_slugs = len(slug_list)

# Initialize an empty list to store the data
all_data = []




for i in range(0, total_slugs, batch_size):
    slug_batch = slug_list[i:i + batch_size]
    response = fetch_cryptocurrency_info(slug_batch)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        data = response.json()
        # Extract the relevant data and append to the list
        if 'data' in data:
            for key, value in data['data'].items():
                all_data.append(value)
    else:
        print(f"Error: {response.status_code} - {response.text}")
    
    # Wait for 1 second before the next request
    time.sleep(2)
