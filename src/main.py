# main.py
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import json

# Load environment variables from a .env file
load_dotenv()

# Function to authenticate and fetch data from the API
def fetch_data(api_key, api_host, api_url, query):

    url = api_url

    querystring = query

    headers = {
	"X-RapidAPI-Key": api_key,
	"X-RapidAPI-Host": api_host
    }
    print("URL: ", url)
    print("Request Headers: ", headers)

    response = requests.get(url, headers=headers, params = querystring)

    #api_url = "https://api.football-data.org/v2/your-endpoint"  # Replace with your actual API endpoint
    #headers = {"X-Auth-Token": api_key}
    #response = requests.get(api_url, headers=headers)

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
    api_url = os.getenv("X-RapidAPI-PlayersURL") 
    print("Fetched Env")
    if not api_key:
        raise ValueError("API key not found. Please set X-RapidAPI-Key in your environment.")
    
    print("Fetching Data")

    player_query = {"id":"276","season":"2020"}

    # Step 2: Data Retrieval
    raw_data = fetch_data(api_key, api_host, api_url, player_query)

    if raw_data:
        # Print the results of the fetch_data function
        print("Results of fetch_data:")
        print(json.dumps(raw_data, indent=2))

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
