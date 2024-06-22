import http.client
import json

def fetch_seasons():
    conn = http.client.HTTPSConnection("api-football-v1.p.rapidapi.com")

    headers = {
        'x-rapidapi-key': "09ae81046bmsh120ce09c82fee6fp1b64b1jsne80fe6af0bcb",
        'x-rapidapi-host': "api-football-v1.p.rapidapi.com"
    }

    conn.request("GET", "/v3/leagues/seasons", headers=headers)

    res = conn.getresponse()
    data = res.read()

    # Step 1: Parse the JSON
    parsed_data = json.loads(data)

    # Extract the list of seasons
    seasons = parsed_data["response"]

    # Step 2: Create a table structure
    table = []

    # Step 3: Populate the table
    for i, season in enumerate(seasons, start=1):
        table.append({"id": i, "season": season})

    return table