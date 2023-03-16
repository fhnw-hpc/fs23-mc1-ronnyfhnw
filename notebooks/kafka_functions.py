from kafka import KafkaProducer, KafkaConsumer, KafkaClient
import json
import pickle
import uuid
import requests
import time
from kafka.admin import KafkaAdminClient, NewPartitions
from kafka.errors import TopicAlreadyExistsError
import kafka
from datetime import datetime
import docker

# defining connections
# server1, server2, server3 = 'broker1:9093', 'broker2:9095', 'broker3:9097'
server1, server2, server3 = 'localhost:9092', 'localhost:9094', 'localhost:9096'
servers = [server1, server2, server3]
binance_topic, twitter_topic = "binance-ws", "twitter"
# binance_n_partitions = 2
    
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

def consume_messages(consumer, n_messages_to_consume:int=1000, timeout_ms:int=2500):
    raw_messages = consumer.poll(timeout_ms=timeout_ms
                                 , max_records=n_messages_to_consume)
    try:
        key = list(raw_messages.keys())[0]
        raw_messages = raw_messages[key]
    
        messages = []
        for raw_message in raw_messages:
            messages.append((raw_message.key, raw_message.value, raw_message.timestamp))
    
        print(f"Consumed {len(messages)} messages from Kafka Cluster")
    except IndexError:
        return None

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
def init_twitter_producer(servers:list=servers, twitter_value_serializer:callable=twitter_value_serializer, twitter_key_serializer:callable=twitter_key_serializer):
    twitter_producer = connect_kafka_producer(servers, twitter_value_serializer, twitter_key_serializer)
    return twitter_producer

# Consumer
def  init_twitter_consumer(servers:list=servers, topic:str=twitter_topic, value_deserializer:callable=twitter_value_deserializer, key_deserializer:callable=twitter_key_deserializer, consumer_timeout_ms:int=3000):
    twitter_consumer = KafkaConsumer(
        topic,
        auto_offset_reset="earliest",
        bootstrap_servers=servers,
        value_deserializer=value_deserializer,
        key_deserializer=key_deserializer,
        consumer_timeout_ms=consumer_timeout_ms
    )

    return twitter_consumer

## JSON Serializer / Deserializer for Binance data
binance_value_serializer, binance_value_deserializer = lambda x: bytes(json.dumps(x), 'utf-8'), lambda x: json.loads(x.decode('utf-8'))
binance_key_serializer, binance_key_deserializer = lambda x: bytes(x, encoding='utf-8'), lambda x: x.decode('utf-8') 

# Producer
def init_binance_producer(servers:list=servers, binance_value_serializer:callable=binance_value_serializer, binance_key_serializer:callable=binance_key_serializer):
    binance_producer = connect_kafka_producer(servers, binance_value_serializer, binance_key_serializer)
    return binance_producer
    
# Consumer
def init_binance_consumer(servers:list=servers, binance_topic:str=binance_topic, binance_value_deserializer:callable=binance_value_deserializer, binance_key_deserializer:callable=binance_key_deserializer, consumer_timeout_ms:int=2000):
    binance_consumer = KafkaConsumer(
        binance_topic, 
        auto_offset_reset='earliest',
        bootstrap_servers=servers,
        api_version=(0, 10), 
        value_deserializer=binance_value_deserializer,
        key_deserializer=binance_key_deserializer,
        consumer_timeout_ms=consumer_timeout_ms
    )
    return binance_consumer


# check if topic exists and number of partitions is set
def create_topic_with_partitions(admin_client, topic_name, partitions):
    """Create a topic with the given number of partitions."""
    topic_partitions = {topic_name: NewPartitions(total_count=partitions)}
    try:
        admin_client.create_topics(new_topics=topic_partitions, validate_only=False)
        print(f"Topic {topic_name} created with {partitions} partitions.")
    except TopicAlreadyExistsError:
        print(f"Topic {topic_name} already exists.")


def check_containers():
    expected_container_names = ['zookeeper1', 'broker1', 'broker2', 'broker3']

    client = docker.from_env()
    containers = client.containers.list(all=True, filters={'name':expected_container_names})
    expected_container_names = [container.name for container in containers]
    running_containers = client.containers.list(filters={'name':expected_container_names})
    available_containers = [container.name for container in running_containers]

    for name in expected_container_names:
        if name not in available_containers:
            print(f"{name} is not running")        
            print(f"restarting {name} ...")        
            client.containers.get(name).restart()
            print(f"restarted {name}")
        else:
            print(f"{name} is running")

def check_kafka(topic:str):
    consumer = None
    while consumer == None:
        try:
            consumer = kafka.KafkaConsumer(topic, bootstrap_servers=servers)
        except kafka.errors.NoBrokersAvailable:
            print("Kafka brokers not available, retrying in 10 seconds ...")
            time.sleep(10)
        except kafka.errors.UnrecognizedBrokerVersion:
            print("Kafka brokers not available, retrying in 10 seconds ...")