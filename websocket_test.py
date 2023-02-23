import websocket
import json

def on_open(ws):
    print("Connection opened")

    # Subscribe to BTC/USDT ticker stream
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@ticker"
        ],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))

def on_message(ws, message):
    print(message)

def on_close(ws):
    print("Connection closed")

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
