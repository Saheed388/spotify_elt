import json
import requests
import pandas as pd
from secret_file import spotify_user_id
from datetime import datetime, timedelta
from refresh_token import Refresh
from pymongo import MongoClient
from bson.objectid import ObjectId

class SaveSongs:
    def __init__(self):
        self.user_id = spotify_user_id
        self.spotify_token = ""
        self.tracks = ""

    def get_recently_played(self):
        # Convert time to Unix timestamp in milliseconds
        today = datetime.now()
        past_7_days = today - timedelta(days=8)
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
        return r.json()

    def call_refresh(self):
        print("Refreshing token")
        refreshCaller = Refresh()
        self.spotify_token = refreshCaller.refresh()

a = SaveSongs()
a.call_refresh()

data = a.get_recently_played()

song_list = []
for song in data["items"]:
    song_data = {
        "song_name": song["track"]["name"],
        "artist_name": song["track"]["album"]["artists"][0]["name"],
        "featured_artists": [artist["name"] for artist in song["track"]["artists"][1:]],
        "played_at": song["played_at"],
        "timestamp": song["played_at"][0:10],
        "popularity": song["track"]["popularity"],
        "album_or_single": song["track"]["album"]["album_type"]
    }
    song_list.append(song_data)

mongo_url = "mongodb+srv://saheedjimoh338:4PKly3XVzJuPBxk4@cluster0.mwjyo1z.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(mongo_url)
db = client.spotify
collection = db.spotify_data

collection.insert_many(song_list)

client.close()
