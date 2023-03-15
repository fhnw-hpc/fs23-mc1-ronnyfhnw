from datetime import datetime
import zmq
import json

def timestamp():
    return datetime.now().strftime("%d-%m-%Y %H:%M:%S")