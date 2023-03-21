# %%
from datetime import datetime
import websocket
import uuid
import json
import zmq
import time
from zmq_functions import *
# %%
with open("secrets.env", "r") as f:
    secrets = json.loads(f.read())

binance_server_url = secrets['binance_producer_url']

# %% 
context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.bind(binance_server_url)

SOCKET_CONNECTED = False

poller = zmq.Poller()
poller.register(socket, zmq.POLLOUT)

print(f"{timestamp()} | waiting for connection from consumer ..")
while SOCKET_CONNECTED == False:
    if poller.poll(timeout=1000):
        print(f"{timestamp()} | Connection accepted at {binance_server_url}")
        SOCKET_CONNECTED = True
    else:
        print(f"{timestamp()} | waiting for connection from consumer")
# %%
def on_open(ws):
    print("Connection opened")
    # Subscribe to different ticker streams
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@ticker",
            "ethusdt@ticker",
            "maticusdt@ticker",
            "shibusdt@ticker",
            "solusdt@ticker"
        ],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))

def on_message(ws, message:str):
    socket.send(message.encode('utf-8'))
    print(f"{timestamp()} | publishing message : {message[:30]}...")

def on_close(ws):
    print("Connection closed")

# %%
if __name__ == "__main__":
    # Initialize WebSocket connection
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/ws",
        on_open=on_open,
        on_message=on_message,
        on_close=on_close
    )
    # Start WebSocket connection
    ws.run_forever()