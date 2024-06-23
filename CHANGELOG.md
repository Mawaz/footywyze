## Detailed Changes - 2024-06-23

```
diff --git a/src/countries.py b/src/countries.py
new file mode 100644
index 0000000..983ae74
--- /dev/null
+++ b/src/countries.py
@@ -0,0 +1,2 @@
+def fetch_countries():
+    pass
\ No newline at end of file
diff --git a/src/leagues.py b/src/leagues.py
new file mode 100644
index 0000000..2054ce7
--- /dev/null
+++ b/src/leagues.py
@@ -0,0 +1,2 @@
+def fetch_leagues():
+    pass
\ No newline at end of file
diff --git a/src/main.py b/src/main.py
index e88b1ad..4e568bd 100644
--- a/src/main.py
+++ b/src/main.py
@@ -8,6 +8,11 @@ from flask import Flask, request
 import psycopg2
 from psycopg2.extras import Json
 import sys
+from countries import *
+from leagues import *
+from teams import *
+from players import *
+from seasons import *
 
 # Load environment variables from a .env file
 load_dotenv()
@@ -53,10 +58,31 @@ def visualize_data(data):
 
 # Fetch and process data
 def data_fetch(client, data_type, query):
-    print(f"Fetching {data_type} Data")
-    api_url = os.getenv(f"X-RapidAPI-{data_type}-URL")
-    data = client.fetch_data(api_url, query)
-    return data
+    if data_type == 'seasons':
+        print("Fetching Seasons Data")
+        data = fetch_seasons(api_url, query)
+        return data
+    elif data_type == 'leagues':
+        print("Fetching Leagues Data")
+        data = fetch_leagues(api_url, query)
+        return data
+    elif data_type == 'teams':
+        print("Fetching Teams Data")
+        data = fetch_teams(api_url, query)
+        return data
+    elif data_type == 'players':
+        print("Fetching Players Data")
+        data = fetch_players(api_url, query)
+        return data
+    elif data_type == 'countries':
+        print("Fetching Countries Data")
+        data = fetch_countries(api_url, query)
+        return data
+    else:
+        print(f"Fetching {data_type} Data")
+        api_url = os.getenv(f"X-RapidAPI-{data_type}-URL")
+        data = client.fetch_data(api_url, query)
+        return data
 
 def extract_data(fetched_data, keys):
     extracted_data = {key: [] for key in keys}
@@ -83,7 +109,7 @@ def flatten_dict(d, parent_key='', sep='_'):
                 items.append((new_key, v))
     return dict(items)
 
-def create_and_insert_data_to_db(table_name, column_dict, data):
+def create_and_insert_data_to_db(table_schema, table_name, column_dict, data):
     # Connect to your postgres DB
     conn = psycopg2.connect(
         dbname=os.getenv("POSTGRESDB_NAME"),
@@ -99,7 +125,7 @@ def create_and_insert_data_to_db(table_name, column_dict, data):
     # Create the table if it doesn't exist
     columns = ', '.join(f'{col} {type}' for col, type in column_dict.items())
     try:
-        cur.execute(f"CREATE TABLE IF NOT EXISTS players.{table_name} ({columns})")
+        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_schema}.{table_name} ({columns})")
         conn.commit()
         print(f"Table {table_name} created successfully")
     except Exception as e:
@@ -107,11 +133,13 @@ def create_and_insert_data_to_db(table_name, column_dict, data):
 
     
     
-    # Flatten the list of lists into a single list
-    flat_list = [item for sublist in data for item in sublist]
-
-    # Flatten each dictionary in the list
-    flat_data_list = [flatten_dict(item) for item in flat_list]
+    if table_name == 'player':
+        # The data is a list of dictionaries
+        flat_data_list = [flatten_dict(item) for item in data]
+    else:
+        # The data is a list of lists of dictionaries
+        flat_list = [item for sublist in data for item in sublist]
+        flat_data_list = [flatten_dict(item) for item in flat_list]
 
     # Now you can loop through flat_data_list and insert each dictionary into the database
     for flat_data in flat_data_list:
@@ -123,11 +151,11 @@ def create_and_insert_data_to_db(table_name, column_dict, data):
             placeholders = ', '.join(['%s'] * len(flat_data))
 
             # Create the UPDATE clause
-            update_clause = ', '.join([f"{column} = EXCLUDED.{column}" for column in flat_data.keys()])
+            #update_clause = ', '.join([f"{column} = EXCLUDED.{column}" for column in flat_data.keys()])
 
             # Execute the query
             #cur.execute(f"INSERT INTO players.{table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {update_clause}", list(flat_data.values()))
-            cur.execute(f"INSERT INTO players.{table_name} ({columns}) VALUES ({placeholders})", list(flat_data.values()))
+            cur.execute(f"INSERT INTO players.{table_name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING", list(flat_data.values()))
             conn.commit()
             #cur.close()
             #conn.close()
@@ -165,11 +193,10 @@ def main(table_name):
 
     table_list = os.getenv(f"{table_name}-TABLES").split(',')
 
-    print(f"Following is the list of tables in {table_name}")
+    print(f"Following is the list of tables  in {table_name}")
 
     for tbl in table_list:
         print(f"{tbl}")
-
     
     print("Fetched Env")
     if not api_key:
diff --git a/src/players.py b/src/players.py
new file mode 100644
index 0000000..52042f9
--- /dev/null
+++ b/src/players.py
@@ -0,0 +1,2 @@
+def fetch_players():
+    pass
\ No newline at end of file
diff --git a/src/seasons.py b/src/seasons.py
new file mode 100644
index 0000000..b0ff84e
--- /dev/null
+++ b/src/seasons.py
@@ -0,0 +1,30 @@
+import http.client
+import json
+
+def fetch_seasons():
+    conn = http.client.HTTPSConnection("api-football-v1.p.rapidapi.com")
+
+    headers = {
+        'x-rapidapi-key': "09ae81046bmsh120ce09c82fee6fp1b64b1jsne80fe6af0bcb",
+        'x-rapidapi-host': "api-football-v1.p.rapidapi.com"
+    }
+
+    conn.request("GET", "/v3/leagues/seasons", headers=headers)
+
+    res = conn.getresponse()
+    data = res.read()
+
+    # Step 1: Parse the JSON
+    parsed_data = json.loads(data)
+
+    # Extract the list of seasons
+    seasons = parsed_data["response"]
+
+    # Step 2: Create a table structure
+    table = []
+
+    # Step 3: Populate the table
+    for i, season in enumerate(seasons, start=1):
+        table.append({"id": i, "season": season})
+
+    return table
\ No newline at end of file
diff --git a/src/teams.py b/src/teams.py
new file mode 100644
index 0000000..261038a
--- /dev/null
+++ b/src/teams.py
@@ -0,0 +1,2 @@
+def fetch_teams():
+    pass
\ No newline at end of file

```

