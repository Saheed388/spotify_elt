import json
import requests
import pandas as pd
from secret_file import spotify_user_id
from datetime import datetime, timedelta
from google.cloud import bigquery

class SaveSongs:
    def __init__(self):
        self.user_id = spotify_user_id
        self.spotify_token = ""
        self.tracks = ""

    def get_recently_played(self):
        # Convert time to Unix timestamp in milliseconds
        today = datetime.now()
        past_7_days = today - timedelta(days=8)
        print(int(past_7_days.timestamp()))
        past_7_days_unix_timestamp = int(past_7_days.timestamp()) * 1000
        # Download all songs you've listened to "after yesterday," which means in the last 24 hours
        endpoint = "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(
            time=past_7_days_unix_timestamp)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self.spotify_token)
        }
        r = requests.get(endpoint, headers=headers, params={"limit": 50})
        if r.status_code not in range(200, 299):
            return {}
        print(r.json())
        return r.json()

    def call_refresh(self):
        print("Refreshing token")
        refreshCaller = Refresh()
        self.spotify_token = refreshCaller.refresh()
        # self.get_recently_played()

a = SaveSongs()
a.call_refresh()

data = a.get_recently_played()

# Define BigQuery settings
project_id = "your-project-id"
dataset_id = "your-dataset-id"
table_id = "your-table-id"

# Create BigQuery client
bq_client = bigquery.Client(project=project_id)

# Define the BigQuery dataset and table
dataset_ref = bq_client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

# Convert the JSON data to a list of rows
rows = []
for song in data["items"]:
    rows.append(
        {
            "song_name": song["track"]["name"],
            "artist_name": song["track"]["album"]["artists"][0]["name"],
            "featured_artists": [artist["name"] for artist in song["track"]["artists"][1:]],
            "played_at": song["played_at"],
            "timestamp": song["played_at"][0:10],
            "popularity": song["track"]["popularity"],
            "album_or_single": song["track"]["album"]["album_type"]
        }
    )

# Load the data into BigQuery
bq_client.insert_rows_json(table_ref, rows)

print("Data loaded successfully into BigQuery table.")
