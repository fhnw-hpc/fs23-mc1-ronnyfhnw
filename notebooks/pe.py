from datetime import datetime
import numpy as np
import pandas as pd
import os
import time
from kafka_functions import *

check_kafka("pe")
pe_consumer = init_pe_consumer()

start = datetime.now()
if __name__ == '__main__':
    while True:
        # determine and load file
        filename = "data/pe_" + datetime.now().strftime("%d-%m-%Y") + ".pkl"

        if (datetime.now() - start).seconds >= 3:
            messages = consume_messages(pe_consumer)
            if messages != None:
                start = datetime.now()

                tmp_df = pd.DataFrame(columns=['measurement_id', 'start', 'end', 'duration', 'service_name', 'process_name'])
                for message in messages:
                    tmp_df = pd.concat((tmp_df, pd.DataFrame(json.loads(message[1]), index=[0])))
                    
                # append to file
                if filename.split("data/")[1] in os.listdir("data/"):
                    tmp_df = pd.concat((pd.read_pickle(filename), tmp_df))
                    tmp_df.to_pickle(filename)
                else:
                    tmp_df.to_pickle(filename)
            else:
                start = datetime.now()

        elif 3 - (datetime.now() - start).seconds > 1:
            sleep_time = 3 - (datetime.now() - start).seconds
            print(f"Sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)