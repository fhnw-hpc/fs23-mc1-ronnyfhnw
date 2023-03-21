# %%
from datetime import datetime
import h5py
import os
import pandas as pd
import websocket
import uuid
import json
import zmq
from zmq_functions import *
import time
# %%
with open("secrets.env", "r") as f:
    secrets = json.loads(f.read())

binance_server_url = secrets['binance_producer_url_connect']

data_processing_interval_s = 5
timeout_s = 10
max_messages = 100


context = zmq.Context()
print(f"{timestamp()} | Connecting to binance server…")
socket = context.socket(zmq.PULL)
socket.connect(binance_server_url)
print(f"{timestamp()} | Connected to binance server")


start = datetime.now()
if __name__ == '__main__':
    while True:
        filename = "data/binance/binance_" + datetime.now().strftime("%d-%m-%Y") + ".h5"

        if (datetime.now() - start).seconds >= data_processing_interval_s:
            recv_start = datetime.now()
            n_messages = 0
            column_names = ['EventType', 'EventTime', 'Symbol', 'PriceChange', 'PriceChangePercent', 'WeightedAveragePrice', 'FirstTrade', 'LastPrice', 'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice', 'BestAskQuantity', 'OpenPrice', 'HighPrice', 'LowPrice', 'TotalTradedBaseAssetVolume', 'TotalTradedQuoteAssetVolume', 'StatsOpenTime', 'StatsCloseTime', 'FirstTradeId', 'LastTradeId', 'number of trades']
            binance_df = pd.DataFrame(columns=column_names)

            process_start = datetime.now()

            # receiving and processing messages
            while (datetime.now() - recv_start).seconds < timeout_s and n_messages < max_messages:
                last_message_received_at = datetime.now()
                message = json.loads(socket.recv().decode('utf-8'), object_hook=dict)
                waiting_time_ms = (datetime.now() - last_message_received_at).total_seconds() * 1000
                print(f"{timestamp()} | Received {str(message)[:30]}")

                # removing starting message
                if 'e' in list(message.keys()):
                    to_append = pd.DataFrame([message])
                    to_append.columns = column_names
                    binance_df = pd.concat((binance_df, to_append))
                if waiting_time_ms > 50:
                    break
                

            # changing dtypes
            columns_to_float = ['PriceChange', 'PriceChangePercent', 'WeightedAveragePrice', 'FirstTrade', 'LastPrice', 'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice', 'BestAskQuantity', 'OpenPrice', 'HighPrice', 'LowPrice', 'TotalTradedBaseAssetVolume', 'TotalTradedQuoteAssetVolume']
            for column in columns_to_float:
                binance_df[column] = binance_df[column].astype(float)

            if filename[13:] in os.listdir("data/binance/"):
                with h5py.File(filename, "a") as hf:
                    for symbol in list(binance_df.Symbol.unique()):
                        symbol_dict = {}
                        symbol_df = binance_df[binance_df.Symbol == symbol]
                        symbol_df.sort_values(by='EventTime', ascending=False)

                        # setting values
                        symbol_dict['EventType'] = symbol_df.EventTime.values[-1] # selecting "oldest" value
                        symbol_dict['Symbol'] = symbol_df.Symbol.values[0]
                        symbol_dict['PriceChange'] = symbol_df.PriceChange.values[0] - symbol_df.PriceChange.values[-1] # calculating diff from oldest to newest value
                        symbol_dict['MeanPriceChange'] = symbol_df.PriceChange.mean()
                        symbol_dict['WeightedAveragePrice'] = symbol_df.WeightedAveragePrice.mean()
                        symbol_dict['LastPrice'] = symbol_df.LastPrice.values[0]
                        symbol_dict['LastQuantity'] = symbol_df.LastQuantity.values[0]
                        symbol_dict['BestBidPrice'] = symbol_df.BestBidPrice.mean()
                        symbol_dict['BestBidQuantity'] = symbol_df.BestBidQuantity.mean()
                        symbol_dict['BestAskPrice'] = symbol_df.BestAskPrice.mean()
                        symbol_dict['BestAskQuantity'] = symbol_df.BestAskQuantity.mean()
                        symbol_dict['OpenPrice'] = symbol_df.OpenPrice.values[-1]
                        symbol_dict['HighPrice'] = symbol_df.HighPrice.max()
                        symbol_dict['MeanHighPrice'] = symbol_df.HighPrice.mean()
                        symbol_dict['LowPrice'] = symbol_df.LowPrice.min()
                        symbol_dict['MeanLowPrice'] = symbol_df.LowPrice.mean()
                        symbol_dict['TotalTradedBaseAssetVolume'] = symbol_df.TotalTradedBaseAssetVolume.sum()
                        symbol_dict['TotalTradedQuoteAssetVolume'] = symbol_df.TotalTradedQuoteAssetVolume.sum()
                        symbol_dict['number of trades'] = symbol_df['number of trades'].sum()

                        group = hf[symbol]

                        for key in symbol_dict.keys():
                            dataset = group[key]
                            n_entries = len(dataset)
                            dataset.resize((n_entries + 1,))
                            dataset[n_entries] = symbol_dict[key]
                print("Appended to file")

            else:
                with h5py.File(filename, "w") as hf:
                    for symbol in list(binance_df.Symbol.unique()):
                        symbol_dict = {}
                        symbol_df = binance_df[binance_df.Symbol == symbol]
                        symbol_df.sort_values(by='EventTime', ascending=False)

                        # setting values
                        symbol_dict['EventType'] = symbol_df.EventTime.values[-1] # selecting "oldest" value
                        symbol_dict['Symbol'] = symbol_df.Symbol.values[0]
                        symbol_dict['PriceChange'] = symbol_df.PriceChange.values[0] - symbol_df.PriceChange.values[-1] # calculating diff from oldest to newest value
                        symbol_dict['MeanPriceChange'] = symbol_df.PriceChange.mean()
                        symbol_dict['WeightedAveragePrice'] = symbol_df.WeightedAveragePrice.mean()
                        symbol_dict['LastPrice'] = symbol_df.LastPrice.values[0]
                        symbol_dict['LastQuantity'] = symbol_df.LastQuantity.values[0]
                        symbol_dict['BestBidPrice'] = symbol_df.BestBidPrice.mean()
                        symbol_dict['BestBidQuantity'] = symbol_df.BestBidQuantity.mean()
                        symbol_dict['BestAskPrice'] = symbol_df.BestAskPrice.mean()
                        symbol_dict['BestAskQuantity'] = symbol_df.BestAskQuantity.mean()
                        symbol_dict['OpenPrice'] = symbol_df.OpenPrice.values[-1]
                        symbol_dict['HighPrice'] = symbol_df.HighPrice.max()
                        symbol_dict['MeanHighPrice'] = symbol_df.HighPrice.mean()
                        symbol_dict['LowPrice'] = symbol_df.LowPrice.min()
                        symbol_dict['MeanLowPrice'] = symbol_df.LowPrice.mean()
                        symbol_dict['TotalTradedBaseAssetVolume'] = symbol_df.TotalTradedBaseAssetVolume.sum()
                        symbol_dict['TotalTradedQuoteAssetVolume'] = symbol_df.TotalTradedQuoteAssetVolume.sum()
                        symbol_dict['number of trades'] = symbol_df['number of trades'].sum()

                        group = hf.create_group(symbol)

                        for key in symbol_dict.keys():
                            if key == "Symbol":
                                group.create_dataset(key, (1,), maxshape=(None,), dtype=h5py.string_dtype(encoding='utf-8'))
                            else:
                                group.create_dataset(key, (1,), maxshape=(None,))
                            group[key][:] = symbol_dict[key]
                print("Wrote to file")
            print(f"finished processing in {datetime.now() - process_start}")
            start = datetime.now()