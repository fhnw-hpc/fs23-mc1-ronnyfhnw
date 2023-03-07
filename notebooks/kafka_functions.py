from kafka import KafkaProducer, KafkaConsumer
import json

# defining connection
server1, server2, server3 = 'broker1:9093', 'broker2:9095', 'broker3:9097'
servers = [server1, server2, server3]
binance_topic, twitter_topic = "binance-ws", "twitter"

# using functions from example notebook
def connect_kafka_producer(servers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=servers, api_version=(0, 10))
        print("Created Producer")
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
    
def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def consume_xy(consumer, topic_name):
    messages = []
    for msg in consumer:
        messages.append((msg.key.decode('utf-8'), msg.value))
    print(f"Consumed {len(messages)} messages from Kafka Cluster")
    return messages

def connect_kafka_producer(servers):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=servers, api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

# setting up consumer for binance
binance_consumer = KafkaConsumer(binance_topic, 
                         auto_offset_reset='earliest',
                         bootstrap_servers=servers,
                         api_version=(0, 10), 
                         value_deserializer = json.loads,
                         consumer_timeout_ms=1000)

# setting up new consumer for twitter messages
twitter_consumer = KafkaConsumer(twitter_topic, 
                         auto_offset_reset='earliest',
                         bootstrap_servers=servers,
                         api_version=(0, 10), 
                         value_deserializer = json.loads,
                         consumer_timeout_ms=1000)

producer = connect_kafka_producer(servers)