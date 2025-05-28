#!/usr/bin/env python3

import sys
import time
import requests
import json
from datetime import datetime
import os

client_secret = os.environ['client_secret']
client_id = os.environ['client_id']
access_token = ""
vehicle_positions_url = os.environ['vehicle_positions_url']
login_url = "https://vdv.stat.gov.lt/multipass/api/oauth2/token"
postUri = os.environ['postUri']
data_context = os.environ['data_context']
update_frequency = 5
debug_mode = os.environ.get("debug_mode", "false").lower() == "true"

def get_realtime_bus_data():
    response = requests.get(vehicle_positions_url)
    if(debug_mode):
        print(response.status_code)
        print(response.text)
    return response.text


def refresh_access_token():
    global access_token
    token_response = requests.post(login_url,
                                   data={
                                       "grant_type": "client_credentials",
                                       "client_id": client_id,
                                       "client_secret": client_secret,
                                       "scope": "api:streams-write"
                                   },
                                   headers={
                                       "Content-Type": "application/x-www-form-urlencoded",
                                   }
                                   )
    access_token = token_response.json()["access_token"]
    print("Token refreshed")



def push_row():

    global access_token

    row_to_add = {
        "timestamp": datetime.now().timestamp() * 1000,
        "data": get_realtime_bus_data(),
        "context": data_context
    }

    def make_request():
        return requests.post(
            postUri,
            data=json.dumps({"records": [row_to_add]}),
            headers={
                "Authorization": "Bearer " + access_token,
                "Content-Type": "application/json",
            },
        )

    response = make_request()

    if(debug_mode):
        print(response.status_code)
        print(response.text)

    if response.status_code == 401:
        print("Access token expired, refreshing...")
        refresh_access_token()
        response = make_request()

    return response


def main():
    print("Starting ingestion")
    try:
        while True:
            start_time = time.time()
            try:
                push_row()
            except Exception as e:
                print(f"Error while pushing row: {e}")

            elapsed = time.time() - start_time
            to_sleep = update_frequency - elapsed
            if to_sleep > 0:
                time.sleep(to_sleep)

    except KeyboardInterrupt:
        print("\nScript interrupted by user. Exiting...")
        sys.exit(0)

if __name__ == "__main__":
    main()