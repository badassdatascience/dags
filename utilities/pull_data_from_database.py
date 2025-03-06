#
# load useful libraries
#
import uuid
from sqlalchemy import create_engine
import pandas as pd

from get_database_connection_string import db_connection_str
from get_sql_for_pull import get_candlestick_pull_query

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
    sql_query_to_run = sql_query_to_run % (price_type_name, instrument_name, interval_name)
    db_connection = create_engine(db_connection_str)

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
    output_query_results_directory = '/'.join(
        [
            pipeline_home,
            output_directory_local_to_home,
            query_output_directory_local_to_output_directory,
        ]
    )

    uid = str(uuid.uuid4())
    output_filename = '%s_%s.parquet' % (table_prefix, uid)
    full_output_path = '/'.join([output_query_results_directory, output_filename])
    pdf.to_parquet(full_output_path)

    return {
        'full_output_path' : full_output_path,
        'uid' : uid,
        'output_filename' : output_filename,
    }
