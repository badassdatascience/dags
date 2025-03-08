

import pandas as pd
from forex.pre_training_data_prep.config import config

pandas_dataframe_list = [
    config['directory_output'] + '/' + config['filename_candlesticks_query_results'],
    config['directory_output'] + '/' + config['filename_timezone_added'],
    ]

for item in pandas_dataframe_list:
    print()
    pdf = pd.read_parquet(item)
    print(pdf)

print()



