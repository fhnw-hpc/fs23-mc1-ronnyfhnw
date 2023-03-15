from datetime import datetime
import zmq
import json

def timestamp():
    return datetime.now().strftime("%d-%m-%Y %H:%M:%S")

def send_message(message:dict, socket:zmq.sugar.socket.Socket):
    message = json.dumps(message).encode('utf-8')
    socket.send(message)
    print("sent message")
    message = socket.recv()
    print(message)