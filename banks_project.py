# Code for ETL operations on Country-GDP data

# Importing the required libraries
import pandas as pd
import requests
import sqlite3
import numpy as np
from bs4 import BeautifulSoup
import datetime

# URL of the archived webpage
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './exchange_rate.csv'
new_csv_path = 'current_exchange_rate.csv'


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"{time_stamp} : {message}\n"
    with open('code_log.txt', 'a') as log_file:
        log_file.write(log_entry)

def extract(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the table under the heading 'By market capitalization'
    tables = soup.find_all('table', {'class': 'wikitable'})
    target_table = tables[0]  # Assuming the first table is the correct one

    # Extract table headers
    headers = [header.text.strip() for header in target_table.find_all('th')]

    # Extract table rows
    rows = []
    for row in target_table.find_all('tr')[1:]:  # Skip the header row
        cells = row.find_all('td')
        if len(cells) > 0:
            cells_text = [cell.text.strip() for cell in cells]
            # Remove unwanted characters and convert Market Cap to float
            cells_text[2] = float(cells_text[2].replace('\n', '').replace('$', '').replace(',', ''))
            rows.append(cells_text)

    # Create a DataFrame
    df = pd.DataFrame(rows, columns=headers)
    
    
    return df

def read_exchange_rates(file_path):
    # Read the exchange rates CSV file into a DataFrame
    exchange_df = pd.read_csv(file_path)
    # Convert the DataFrame into a dictionary with currency codes as keys and rates as values
    exchange_dict = exchange_df.set_index('Currency')['Rate'].to_dict()
    return exchange_dict

def transform(df, exchange_dict):
    # Add new columns for Market Cap in different currencies using exchange rates
    df.rename(columns = {'Bank name':'Name'}, inplace = True)
    df.rename(columns = {'Market cap(US$ billion)':'MC_USD_Billion'}, inplace = True)
    df['MC_USD_Billion'] = [np.round(x*exchange_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_dict['GBP'], 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_dict['EUR'], 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_dict['INR'], 2)

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index = False)
    #log_progress(f'DataFrame saved to {csv_path}')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    #log_progress(f'DataFrame saved to {table_name}')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)




# Main function
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url)

exchange_rates_file = csv_path
exchange_dict = read_exchange_rates(exchange_rates_file)
log_progress('Data extraction complete. Initiating Transformation process')

transformed_df = transform(df, exchange_dict)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(transformed_df, new_csv_path)
log_progress('Data has saved to csv file.')

sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

query_statement_1 = f"SELECT * FROM {table_name}"
run_query(query_statement_1, sql_connection)
query_statement_2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement_2, sql_connection)
query_statement_3 = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement_3, sql_connection)

log_progress('Process Complete.')

sql_connection.close()