# main.py
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import json
from flask import Flask, request

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

# Function to run the entire pipeline
def main():
    # Step 1: API Access and Authentication
    print("Started Main")
    api_key = os.getenv("X-RapidAPI-Key")
    api_host = os.getenv("X-RapidAPI-Host") 
    player_api_url = os.getenv("X-RapidAPI-Players-URL") 
    print("Fetched Env")
    if not api_key:
        raise ValueError("API key not found. Please set X-RapidAPI-Key in your environment.")

    client = APIClient(api_key, api_host)

    # Fetch and process data
    def data_fetch(client, data_type, query):
        print(f"Fetching {data_type} Data")
        api_url = os.getenv(f"X-RapidAPI-{data_type}-URL")
        data = client.fetch_data(api_url, query)
        return data
    fetched_data = data_fetch(client, 'Players', {"id":"276","season":"2020"})
    process_data(fetched_data)

    if fetched_data:
        # Print the results of the fetch_data function
        print("Results of fetch_data:")
        print(json.dumps(fetched_data, indent=2))

    #if raw_data:
        # Step 3: Data Storage
    #    save_data_to_file(raw_data, "data/raw/raw_data.json")

        # Step 4: Data Processing
    #    processed_data = process_data(raw_data)

        # Step 5: Data Analysis and Modeling
    #    analyze_and_model(processed_data)

        # Step 6: Data Visualization
    #    visualize_data(processed_data)

    else:
        print("Exiting due to API error.")

if __name__ == "__main__":
    main()
