# %%
from kafka_functions import *
from datetime import datetime
import websocket
import uuid

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

def on_message(ws, message):
    # write data into cluster
    key = str(uuid.uuid4())
    publish_message(binance_producer, binance_topic, key, message)
    print(f"{datetime.now()}: writing message to cluster: {str(message)[:50]}...")

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