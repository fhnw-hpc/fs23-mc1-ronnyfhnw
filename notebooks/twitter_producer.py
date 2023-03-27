# %% 
from kafka_functions import *
from datetime import datetime
import requests
import uuid
import json
import kafka
import time

pe = PerformanceEvaluator("data/twitter/performance_twitter_producer.json", "twitter_producer")

check_kafka(twitter_topic)
twitter_producer = init_twitter_producer()

# %%
url = "https://api.twitter.com/2/tweets/search/recent?query=%23crypto%20OR%20%23ethereum%20OR%20%23eth%20OR%20%23btc%20OR%20%23bitcoin&max_results=100&tweet.fields=created_at,text,author_id"

with open("secrets.env", "r") as f:
    secrets = json.loads(str(f.read()))

bearerToken = secrets['bearerToken']
timestamp_last_request = datetime.now()

# %%
if __name__ == "__main__":
    while True:
        # time limit to stay within api regulations
        if (datetime.now() - timestamp_last_request).seconds > 10:
            # get tweets
            api_id = pe.start("api_call")
            tweets = json.loads(requests.get(url=url, headers={'Authorization': f"Bearer {bearerToken}"}).text)['data']
            pe.end(api_id)

            publish_id = pe.start("publish")
            for tweet in tweets:
                # formatting for json serializing
                formatted_tweet = json.dumps(tweet)
                
                # write to cluster
                key = str(uuid.uuid4())
                publish_message(twitter_producer, twitter_topic, key, formatted_tweet)
                print(f"{datetime.now()}: writing message to cluster: {formatted_tweet[:50]}...")
                
            pe.end(publish_id)
            timestamp_last_request = datetime.now()
