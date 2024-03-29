from datetime import datetime, timedelta
import h5py
import pandas as pd
import os
import dask.dataframe as dd
from kafka_functions import *

interval_length = 3

pe_producer = init_pe_producer()
pe = PerformanceEvaluator("binance_processor", pe_producer)

check_kafka(binance_topic)

binance_consumer = init_binance_consumer()
print("Starting Binance Processor")

start = datetime.now()
if __name__ == "__main__":
    while True:
        filename = "data/binance/binance_" + datetime.now().strftime("%d-%m-%Y") + ".h5"

        if (datetime.now() - start).seconds >= interval_length:
            consume_id = pe.start("consume")
            consumed_messages = consume_messages(binance_consumer)
            pe.end(consume_id)

            column_names = ['EventType', 'EventTime', 'Symbol', 'PriceChange', 'PriceChangePercent', 'WeightedAveragePrice', 'FirstTrade', 'LastPrice', 'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice', 'BestAskQuantity', 'OpenPrice', 'HighPrice', 'LowPrice', 'TotalTradedBaseAssetVolume', 'TotalTradedQuoteAssetVolume', 'StatsOpenTime', 'StatsCloseTime', 'FirstTradeId', 'LastTradeId', 'number of trades']
            binance_df = pd.DataFrame(columns=column_names)

            process_start = datetime.now()

            process_id = pe.start("process")
            try:
                for message in consumed_messages:
                    value = json.loads(message[1])

                    # removes starting connection messages
                    if 'e' in value.keys():
                        to_append = pd.DataFrame([value])
                        to_append.columns = column_names
                        binance_df = pd.concat((binance_df, to_append))

                # changing dtypes
                columns_to_float = ['PriceChange', 'PriceChangePercent', 'WeightedAveragePrice', 'FirstTrade', 'LastPrice', 'LastQuantity', 'BestBidPrice', 'BestBidQuantity', 'BestAskPrice', 'BestAskQuantity', 'OpenPrice', 'HighPrice', 'LowPrice', 'TotalTradedBaseAssetVolume', 'TotalTradedQuoteAssetVolume']
                for column in columns_to_float:
                    binance_df[column] = binance_df[column].astype(float)

                data_written = False
                while data_written == False:
                    if filename.split("binance/")[1] in os.listdir("data/binance/"):
                        try:
                            with h5py.File(filename, "a") as hf:
                                for symbol in list(binance_df.Symbol.unique()):
                                    symbol_dict = {}
                                    symbol_df = binance_df[binance_df.Symbol == symbol]
                                    symbol_df.sort_values(by='EventTime', ascending=False)

                                    symbol_dd = dd.from_pandas(symbol_df, npartitions=8)

                                    # setting values
                                    symbol_dict['EventType'] = symbol_df.EventTime.values[-1] # selecting "oldest" value
                                    symbol_dict['Symbol'] = symbol_df.Symbol.values[0]
                                    symbol_dict['PriceChange'] = symbol_df.PriceChange.values[0] - symbol_df.PriceChange.values[-1] # calculating diff from oldest to newest value
                                    symbol_dict['MeanPriceChange'] = symbol_dd.PriceChange.mean().compute()
                                    symbol_dict['WeightedAveragePrice'] = symbol_dd.WeightedAveragePrice.mean().compute()
                                    symbol_dict['LastPrice'] = symbol_df.LastPrice.values[0]
                                    symbol_dict['LastQuantity'] = symbol_df.LastQuantity.values[0]
                                    symbol_dict['BestBidPrice'] = symbol_dd.BestBidPrice.mean().compute()
                                    symbol_dict['BestBidQuantity'] = symbol_dd.BestBidQuantity.mean().compute()
                                    symbol_dict['BestAskPrice'] = symbol_df.BestAskPrice.mean()
                                    symbol_dict['BestAskQuantity'] = symbol_dd.BestAskQuantity.mean().compute()
                                    symbol_dict['OpenPrice'] = symbol_df.OpenPrice.values[-1]
                                    symbol_dict['HighPrice'] = symbol_dd.HighPrice.max().compute()
                                    symbol_dict['MeanHighPrice'] = symbol_dd.HighPrice.mean().compute()
                                    symbol_dict['LowPrice'] = symbol_dd.LowPrice.min().compute()
                                    symbol_dict['MeanLowPrice'] = symbol_dd.LowPrice.mean().compute()
                                    symbol_dict['TotalTradedBaseAssetVolume'] = symbol_dd.TotalTradedBaseAssetVolume.sum().compute()
                                    symbol_dict['TotalTradedQuoteAssetVolume'] = symbol_dd.TotalTradedQuoteAssetVolume.sum().compute()
                                    symbol_dict['number of trades'] = symbol_dd['number of trades'].sum().compute()

                                    try:
                                        group = hf[symbol]
                                    except KeyError:
                                        hf.create_group(symbol, (1,), maxshape=(None,), dtype=h5py.string_dtype(encoding='utf-8'))

                                    for key in symbol_dict.keys():
                                        dataset = group[key]
                                        n_entries = len(dataset)
                                        dataset.resize((n_entries + 1,))
                                        dataset[n_entries] = symbol_dict[key]
                            print("Appended to file")
                            data_written = True
                        except BlockingIOError:
                            print(f"waiting until file is unlocked ...")
                            time.sleep(0.1)
                    else:
                        try:
                            with h5py.File(filename, "w") as hf:
                                for symbol in list(binance_df.Symbol.unique()):
                                    symbol_dict = {}
                                    symbol_df = binance_df[binance_df.Symbol == symbol]
                                    symbol_df.sort_values(by='EventTime', ascending=False)

                                    symbol_dd = dd.from_pandas(symbol_df, npartitions=8)

                                    # setting values
                                    symbol_dict['EventType'] = symbol_df.EventTime.values[-1] # selecting "oldest" value
                                    symbol_dict['Symbol'] = symbol_df.Symbol.values[0]
                                    symbol_dict['PriceChange'] = symbol_df.PriceChange.values[0] - symbol_df.PriceChange.values[-1] # calculating diff from oldest to newest value
                                    symbol_dict['MeanPriceChange'] = symbol_dd.PriceChange.mean().compute()
                                    symbol_dict['WeightedAveragePrice'] = symbol_dd.WeightedAveragePrice.mean().compute()
                                    symbol_dict['LastPrice'] = symbol_df.LastPrice.values[0]
                                    symbol_dict['LastQuantity'] = symbol_df.LastQuantity.values[0]
                                    symbol_dict['BestBidPrice'] = symbol_dd.BestBidPrice.mean().compute()
                                    symbol_dict['BestBidQuantity'] = symbol_dd.BestBidQuantity.mean().compute()
                                    symbol_dict['BestAskPrice'] = symbol_df.BestAskPrice.mean()
                                    symbol_dict['BestAskQuantity'] = symbol_dd.BestAskQuantity.mean().compute()
                                    symbol_dict['OpenPrice'] = symbol_df.OpenPrice.values[-1]
                                    symbol_dict['HighPrice'] = symbol_dd.HighPrice.max().compute()
                                    symbol_dict['MeanHighPrice'] = symbol_dd.HighPrice.mean().compute()
                                    symbol_dict['LowPrice'] = symbol_dd.LowPrice.min().compute()
                                    symbol_dict['MeanLowPrice'] = symbol_dd.LowPrice.mean().compute()
                                    symbol_dict['TotalTradedBaseAssetVolume'] = symbol_dd.TotalTradedBaseAssetVolume.sum().compute()
                                    symbol_dict['TotalTradedQuoteAssetVolume'] = symbol_dd.TotalTradedQuoteAssetVolume.sum().compute()
                                    symbol_dict['number of trades'] = symbol_dd['number of trades'].sum().compute()

                                    group = hf.create_group(symbol)

                                    for key in symbol_dict.keys():
                                        if key == "Symbol":
                                            group.create_dataset(key, (1,), maxshape=(None,), dtype=h5py.string_dtype(encoding='utf-8'))
                                        else:
                                            group.create_dataset(key, (1,), maxshape=(None,))
                                        group[key][:] = symbol_dict[key]
                            data_written = True
                        except BlockingIOError:
                            print("waiting till file is unlocked")
                            time.sleep(0.1)
                    
                print(f"finished processing in {datetime.now() - process_start}")
                start = datetime.now()
            except TypeError:
                print("no new messages available")
            pe.end(process_id)