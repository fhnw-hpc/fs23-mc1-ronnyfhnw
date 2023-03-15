# %%
from datetime import datetime
import websocket
import uuid
import json
import zmq
import time
import requests
from zmq_functions import *

# %% 
with open("secrets.env", "r") as f:
    secrets = json.loads(f.read())

twitter_producer_url = secrets['twitter_producer_url']
bearerToken = secrets['bearerToken']
url = "https://api.twitter.com/2/tweets/search/recent?query=%23crypto%20OR%20%23ethereum%20OR%20%23eth%20OR%20%23btc%20OR%20%23bitcoin&max_results=100&tweet.fields=created_at,text,author_id"

with open("secrets.env", "r") as f:
    secrets = json.loads(str(f.read()))

# %% [markdown]
# # Bind socket to port and only progress in code if a connection to port was accepted
context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.bind(twitter_producer_url)

monitor_socket = context.socket(zmq.PAIR)
monitor_socket.connect("inproc://monitor")


socket.monitor("inproc://monitor", zmq.EVENT_ACCEPTED)
SOCKET_CONNECTED = False

print(f"{timestamp()} | waiting for connection from consumer ..")
while SOCKET_CONNECTED == False:
    if twitter_producer_url == monitor_socket.recv().decode('utf-8'):
        print(f"{timestamp()} | Connection accepted at {twitter_producer_url}")
        SOCKET_CONNECTED = True

timestamp_last_request = datetime.now()
while True:
    if (datetime.now() - timestamp_last_request).seconds >= 10:
        tweets = json.loads(requests.get(url=url, headers={'Authorization': f"Bearer {bearerToken}"}).text)['data']
        for tweet in tweets:
            formatted_tweet = json.dumps(tweet)            
            socket.send(formatted_tweet.encode('utf-8'))
            print(f"{timestamp()} | Published {formatted_tweet[:30]}  ")

        timestamp_last_request = datetime.now()