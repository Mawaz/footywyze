# main.py
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import json
from flask import Flask, request
import psycopg2
from psycopg2.extras import Json
import sys
from countries import *
from leagues import *
from teams import *
from players import *
from seasons import *

# Load environment variables from a .env file
load_dotenv()

# Class to authenticate and fetch data from the API
class APIClient:
    def __init__(self, api_key, api_host):
        self.api_key = api_key
        self.api_host = api_host
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host
        }

    def fetch_data(self, api_url, query):
        response = requests.get(api_url, headers=self.headers, params=query)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            return None

# Function to save data to a local file (data/raw/)
def save_data_to_file(data, filename):
    # Implement data saving logic (e.g., CSV, JSON, etc.)
    pass

# Function for data processing
def process_data(raw_data):
    # Implement data processing logic
    processed_data = raw_data  # Placeholder, replace with actual processing
    return processed_data

# Function for data analysis and modeling
def analyze_and_model(data):
    # Implement data analysis and modeling logic (e.g., pandas, scikit-learn, etc.)
    pass

# Function for data visualization
def visualize_data(data):
    # Implement data visualization logic (e.g., Matplotlib, Seaborn, etc.)
    pass

# Fetch and process data
def data_fetch(client, data_type, query):
    if data_type == 'seasons':
        print("Fetching Seasons Data")
        data = fetch_seasons(api_url, query)
        return data
    elif data_type == 'leagues':
        print("Fetching Leagues Data")
        data = fetch_leagues(api_url, query)
        return data
    elif data_type == 'teams':
        print("Fetching Teams Data")
        data = fetch_teams(api_url, query)
        return data
    elif data_type == 'players':
        print("Fetching Players Data")
        data = fetch_players(api_url, query)
        return data
    elif data_type == 'countries':
        print("Fetching Countries Data")
        data = fetch_countries(api_url, query)
        return data
    else:
        print(f"Fetching {data_type} Data")
        api_url = os.getenv(f"X-RapidAPI-{data_type}-URL")
        data = client.fetch_data(api_url, query)
        return data

def extract_data(fetched_data, keys):
    extracted_data = {key: [] for key in keys}

    for response in fetched_data['response']:
        for key in keys:
            if key in response:
                extracted_data[key].append(response[key])
            elif 'statistics' in response:
                for stat in response['statistics']:
                    if key in stat:
                        extracted_data[key].append(stat[key])

    return extracted_data

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
    return dict(items)

def create_and_insert_data_to_db(table_schema, table_name, column_dict, data):
    # Connect to your postgres DB
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRESDB_NAME"),
        user=os.getenv("POSTGRESDB_USER"),
        password=os.getenv("POSTGRESDB_PASS"),
        host=os.getenv("POSTGRESDB_HOST"),
        port=os.getenv("POSTGRESDB_PORT")
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Create the table if it doesn't exist
    columns = ', '.join(f'{col} {type}' for col, type in column_dict.items())
    try:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_schema}.{table_name} ({columns})")
        conn.commit()
        print(f"Table {table_name} created successfully")
    except Exception as e:
        print(f"An error occurred: {e}")

    
    
    if table_name == 'player':
        # The data is a list of dictionaries
        flat_data_list = [flatten_dict(item) for item in data]
    else:
        # The data is a list of lists of dictionaries
        flat_list = [item for sublist in data for item in sublist]
        flat_data_list = [flatten_dict(item) for item in flat_list]

    # Now you can loop through flat_data_list and insert each dictionary into the database
    for flat_data in flat_data_list:
        if flat_data:  # Check if flat_data is not empty
            # Get the column names
            columns = ', '.join(flat_data.keys())

            # Create the placeholders
            placeholders = ', '.join(['%s'] * len(flat_data))

            # Create the UPDATE clause
            #update_clause = ', '.join([f"{column} = EXCLUDED.{column}" for column in flat_data.keys()])

            # Execute the query
            #cur.execute(f"INSERT INTO players.{table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {update_clause}", list(flat_data.values()))
            cur.execute(f"INSERT INTO players.{table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING", list(flat_data.values()))
            conn.commit()
            #cur.close()
            #conn.close()


    # Insert the data into the table
    #flat_data = {}
    #print(data[0])
    #for key, value in data[0].items():
    #    if isinstance(value, dict):
    #        for subkey, subvalue in value.items():
    #            flat_data[f"{key}_{subkey}"] = subvalue
    #    else:
    #        flat_data[key] = value
    #placeholders = ', '.join(['%s'] * len(flat_data))
    #columns = ', '.join(flat_data.keys())
    # Create the UPDATE clause
    #update_clause = ', '.join([f"{column} = EXCLUDED.{column}" for column in flat_data.keys()])

    #cur.execute(f"INSERT INTO players.{table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {update_clause}", list(flat_data.values()))

    # Commit the transaction
    #conn.commit()

    # Close the cursor and connection
    #cur.close()
    #conn.close()

# Function to run the entire pipeline
def main(table_name):
    # Step 1: API Access and Authentication
    print("Started Main for " + table_name)
    api_key = os.getenv("X-RapidAPI-Key")
    api_host = os.getenv("X-RapidAPI-Host") 

    table_list = os.getenv(f"{table_name}-TABLES").split(',')

    print(f"Following is the list of tables  in {table_name}")

    for tbl in table_list:
        print(f"{tbl}")
    
    print("Fetched Env")
    if not api_key:
        raise ValueError("API key not found. Please set X-RapidAPI-Key in your environment.")

    client = APIClient(api_key, api_host)

    for season in range(2010, 2025):
        fetched_data = data_fetch(client, table_name, {"id":"276","season":str(season)})

        extracted_data = extract_data(fetched_data, table_list)

        # Assuming player_data is the data for the player table
        print("Extracted Data ID: ", extracted_data['player'][0]['id'])
        master_id = extracted_data['player'][0]['id']

        # Add the player's id to each item in the statistics data
        for sublist in extracted_data['statistics']:
            for item in sublist:
                item['id'] = master_id

        for tbl in table_list:
            print(f"Creating and Inserting Data for {tbl}")
            table_dict_str = os.getenv(f"{tbl}-DICT")
            if table_dict_str:
                print("Current Table Dictionary: ", table_dict_str)
                cur_table_dict = json.loads(table_dict_str)
            else:
                print(f"Environment variable {tbl}-DICT is not set or is empty")
            print("Current Table Dictionary: ", cur_table_dict)
            print("Current Table Data: ", extracted_data[tbl])
            create_and_insert_data_to_db(tbl, cur_table_dict, extracted_data[tbl])

    process_data(fetched_data)

    #if fetched_data:
        # Print the results of the fetch_data function
    #    print("Results of fetch_data:")
    #    print(json.dumps(fetched_data, indent=2))

    #if raw_data:
        # Step 3: Data Storage
    #    save_data_to_file(raw_data, "data/raw/raw_data.json")

        # Step 4: Data Processing
    #    processed_data = process_data(raw_data)

        # Step 5: Data Analysis and Modeling
    #    analyze_and_model(processed_data)

        # Step 6: Data Visualization
    #    visualize_data(processed_data)

    #else:
    #    print("Exiting due to API error.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("No command line argument provided")
