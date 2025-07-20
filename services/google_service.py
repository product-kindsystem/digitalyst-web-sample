import requests
import json

def get_googlemap_api_key():    
    # api_key = "AIzaSyDJy6f3otu6T8D06svVDAoiCEJNX2tu3Dg" # wxpython
    # api_key = "AIzaSyAwMWc_o-2UTlwEETGCq02oKxXPfA1ZKUA" # Kashi
    api_key = "AIzaSyAuohuFJ1PVEuLAIXmEHCTh-QTSCDjEfRw"
    return api_key


def create_googlemap_session(api_key):
    url = "https://tile.googleapis.com/v1/createSession?key=" + api_key
    headers = {"Content-Type": "application/json"}
    data = {"mapType": "satellite","language": "ja-JP","region": "JP"}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        response_data = response.json()
        session = response_data.get("session")
    else:
        session = None
        print(f"Request failed with status code {response.status_code}: {response.text}")
    return session

