from kafka import KafkaProducer, KafkaConsumer
import json
import pickle
import uuid
import requests

# defining connections
# server1, server2, server3 = 'broker1:9093', 'broker2:9095', 'broker3:9097'
server1, server2, server3 = 'localhost:9092', 'localhost:9094', 'localhost:9096'
servers = [server1, server2, server3]
binance_topic, twitter_topic = "binance-ws", "twitter"
    
def publish_message(producer_instance, topic_name, key, value):
    if producer_instance.config['value_serializer'] == None or producer_instance.config['key_serializer'] == None:
        raise ValueError("Serializer not specified")
    try:
        # key_bytes = bytes(key, encoding='utf-8') --> no longer necessary since producers must have a serializer specified
        # value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key, value=value)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def consume_messages(consumer):
    messages = []
    for msg in consumer:
        messages.append((msg.key, msg.value))
    print(f"Consumed {len(messages)} messages from Kafka Cluster")
    return messages    

def connect_kafka_producer(servers, value_serializer, key_serializer):
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=value_serializer,
            key_serializer=key_serializer,
            api_version=(0, 10)
        )
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

## PICKLE Serializer / Deserializer for Twitter data
twitter_value_serializer, twitter_value_deserializer = lambda x: pickle.dumps(x), lambda x: pickle.loads(x)
twitter_key_serializer, twitter_key_deserializer = lambda x: bytes(x, encoding='utf-8'), lambda x: x.decode('utf-8') 

# Producer
twitter_producer = connect_kafka_producer(servers, twitter_value_serializer, twitter_key_serializer)

# Consumer
twitter_consumer = KafkaConsumer(
    twitter_topic,
    auto_offset_reset="earliest",
    bootstrap_servers=servers,
    value_deserializer=twitter_value_deserializer,
    key_deserializer=twitter_key_deserializer,
    consumer_timeout_ms=3000
)

## JSON Serializer / Deserializer for Binance data
binance_value_serializer, binance_value_deserializer = lambda x: bytes(json.dumps(x), 'utf-8'), lambda x: json.loads(x.decode('utf-8'))
binance_key_serializer, binance_key_deserializer = lambda x: bytes(x, encoding='utf-8'), lambda x: x.decode('utf-8') 

# Producer
binance_producer = connect_kafka_producer(servers, binance_value_serializer, binance_key_serializer)

# Consumer
binance_consumer = KafkaConsumer(
    binance_topic, 
    auto_offset_reset='earliest',
    bootstrap_servers=servers,
    api_version=(0, 10), 
    value_deserializer=binance_value_deserializer,
    consumer_timeout_ms=1000
)