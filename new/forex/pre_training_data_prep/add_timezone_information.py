import pandas as pd
import pytz
import datetime

def add_timezone_information(**config):
    tz = pytz.timezone(config['tz_name'])
    
    pdf = (
        pd.read_parquet(
            config['directory_output'] + '/' + config['filename_candlesticks_query_results'],
        )
        .sort_values(by = 'timestamp')
    )

    pdf['datetime_tz'] = [datetime.datetime.fromtimestamp(x, tz) for x in pdf['timestamp']]
    pdf['weekday_tz'] = [datetime.datetime.weekday(x) for x in pdf['datetime_tz']]
    pdf['hour_tz'] = [x.hour for x in pdf['datetime_tz']]
    output_filename = config['directory_output'] + '/' + config['filename_timezone_added']
    pdf.to_parquet(output_filename)



    
