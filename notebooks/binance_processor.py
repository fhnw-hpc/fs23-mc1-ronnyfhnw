# %%
from kafka_functions import *
from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

with open("secrets.env", "r") as f:
    secrets = json.loads(str(f.read()))

servers = secrets['servers']
print(servers)

# %%
# setup kafka consumer
consumer = KafkaConsumer(
    'binance-ws',
    auto_offset_reset="earliest",
    bootstrap_servers=servers,
    api_version=(0,10),
    value_deserializer=json.loads,
    consumer_timeout_ms=1000)

producer = connect_kafka_producer(servers)

# %%
# start = datetime.now() - timedelta(minutes=5)

# while True:
#     if (datetime.now() - start).seconds >= 60:
#         # data processor
#         pass

#     else:
#         # wait until 5 min mark
#         pass
# %%
consumed_messages = consume_xy(consumer, 'binance-ws')
# %%
pd.DataFrame(consumed_messages)

# %%
data = [message[1] for message in consumed_messages]
data = pd.DataFrame(data)
data = data[data['id'] != 1]

# change column names 
data.columns = ['result', 'id', 'EventType', 'EventTime', 'Symbol', 'PriceChange', 'PriceChangePercent', 'WeightedAveragePrice', 'FirstTrade', 'LastPrice', 'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice', 'BestAskQuantity', 'OpenPrice', 'HighPrice', 'LowPrice', 'TotalTradedBaseAssetVolume', 'TotalTradedQuoteAssetVolume', 'StatsOpenTime', 'StatsCloseTime', 'FirstTradeId', 'LastTradeId', 'number of trades']

# dtype transformations
columns_to_float = ['PriceChange', 'PriceChangePercent', 'WeightedAveragePrice', 'FirstTrade', 'LastPrice', 'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice', 'BestAskQuantity', 'OpenPrice', 'HighPrice', 'LowPrice', 'TotalTradedBaseAssetVolume', 'TotalTradedQuoteAssetVolume']
for column in columns_to_float:
    data[column] = data[column].astype(float)

# remove unnecessary columns
data = data[['result', 'id', 'EventType', 'EventTime', 'Symbol', 'PriceChange',
       'PriceChangePercent', 'WeightedAveragePrice', 'FirstTrade', 'LastPrice',
       'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice',
       'BestAskQuantity', 'OpenPrice', 'HighPrice', 'LowPrice',
       'TotalTradedBaseAssetVolume', 'TotalTradedQuoteAssetVolume',
       'StatsOpenTime', 'StatsCloseTime', 'FirstTradeId', 'LastTradeId',
       'number of trades']]
data
# %%
plt.plot(data.PriceChange)
# %%
data.describe().columns
# %%
data.columns
# %%
