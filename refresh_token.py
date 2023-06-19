from secret_file import refresh_token,base_64
import requests
import json


class Refresh:

    def __init__(self):
        self.refresh_token = refresh_token
        self.base_64 = base_64 # client_id:client_secret

    def refresh(self):

        endpoint = "https://accounts.spotify.com/api/token" #url

        response = requests.post(endpoint,
                                 data={"grant_type": "refresh_token",
                                       "refresh_token": refresh_token},
                                 headers={"Authorization": "Basic " + base_64})
        
        # post, get, update

        response_json = response.json()
        print(response_json)
        
        return response_json["access_token"]

a = Refresh()
a.refresh()