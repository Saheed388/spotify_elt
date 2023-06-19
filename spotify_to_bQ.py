import json
import requests
import pandas as pd
from secret_file import spotify_user_id
from datetime import datetime, date, time, timedelta
from refresh_token import Refresh
from google.cloud import storage


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

song_names = []
artist_names = []
featured_artists = []
played_at_list = []
timestmps = []
popularity = []
album_or_single = []

# Extracting only the relevant bits of data from the JSON object
for song in data["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    featured_artists.append([artist["name"] for artist in song["track"]["artists"][1:]])
    played_at_list.append(song["played_at"])
    timestmps.append(song["played_at"][0:10])
    popularity.append(song["track"]["popularity"])
    album_or_single.append(song["track"]["album"]["album_type"])

# Prepare a dictionary in order to turn it into a pandas DataFrame below
song_dict = {
    "song_name": song_names,
    "artist_name": artist_names,
    "featured_artists": featured_artists,
    "played_at": played_at_list,
    "timestamp": timestmps,
    "popularity": popularity,
    "album_or_single": album_or_single
}

song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "featured_artists",
                                           "played_at", "timestamp", "popularity", "album_or_single"])
print(song_df)

csv_filename = "saeed_cloud_spotify_songs_data.csv"
song_df.to_csv(csv_filename, index=False)

# Define the GCS bucket and destination blob
bucket_name = "spotify_dat"
destination_blob_name = "saeed_cloud_spotify_songs_data.csv"

# Upload the CSV file to the GCS bucket
client = storage.Client()
bucket = client.bucket(bucket_name)
blob = bucket.blob(destination_blob_name)
blob.upload_from_filename(csv_filename)

print("Data uploaded successfully to GCS bucket.")
