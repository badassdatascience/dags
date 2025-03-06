#
# load useful libraries
#
import uuid
from sqlalchemy import create_engine
import pandas as pd
import pytz

from get_database_connection_string import db_connection_str
from get_sql_for_pull import get_candlestick_pull_query

#
# temporary user settings
#
pipeline_home = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components'

#
# pull candlesticks into a pandas dataframe
#
def pull_candlesticks_into_pandas_dataframe(
        db_connection_str,
        sql_query_to_run,
        price_type_name = 'mid',
        instrument_name = 'EUR/USD',
        interval_name = 'Minute',
):
    #
    # substitute the "%s" query parameters with their intended values
    #
    sql_query_to_run = sql_query_to_run % (price_type_name, instrument_name, interval_name)
    
    #
    # connect to database
    #
    db_connection = create_engine(db_connection_str)

    #
    # run query, loading contents into a pandas dataframe
    #
    pdf = (
        pd
        .read_sql(
            sql_query_to_run,
            con = db_connection,
        )
        .sort_values(
            by = ['timestamp'],
        )
    )
    pdf.index = pdf['timestamp']
    
    # tz = pytz.timezone(tz_name)
    # pdf['datetime_tz'] = [datetime.datetime.fromtimestamp(x, tz) for x in pdf['timestamp']]
    # pdf['weekday_tz'] = [datetime.datetime.weekday(x) for x in pdf['datetime_tz']]
    # pdf['hour_tz'] = [x.hour for x in pdf['datetime_tz']]
    
    #
    # return the pandas datafrome
    #
    return pdf

# 
# save the Pandas dataframe
#
def save_candlesticks_pandas_dataframe(
        pdf,
        pipeline_home,
        table_prefix = 'candlestick_query_results',
        output_directory_local_to_home = 'output',
        query_output_directory_local_to_output_directory = 'queries',
):

    #
    # compute save directory path (without the final filename yet)
    #
    output_query_results_directory = '/'.join(
        [
            pipeline_home,
            output_directory_local_to_home,
            query_output_directory_local_to_output_directory,
        ]
    )
    
    #
    # save output
    #
    uid = str(uuid.uuid4())
    output_filename = '%s_%s.parquet' % (table_prefix, uid)
    full_output_path = '/'.join([output_query_results_directory, output_filename])
    pdf.to_parquet(full_output_path)
    
    #
    # return something
    #
    return full_output_path

#
# run main
#
if __name__ == '__main__':
    sql_query_for_candlestick_pull = get_candlestick_pull_query()
    pdf = pull_candlesticks_into_pandas_dataframe(db_connection_str, sql_query_for_candlestick_pull)
    full_output_path = save_candlesticks_pandas_dataframe(pdf, pipeline_home)
    print(full_output_path)
    
