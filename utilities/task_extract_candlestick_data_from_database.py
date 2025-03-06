from get_database_connection_string import db_connection_str
from get_sql_for_pull import get_candlestick_pull_query
from pull_data_from_database import pull_candlesticks_into_pandas_dataframe
from pull_data_from_database import save_candlesticks_pandas_dataframe


def task_extract_candlestick_data_from_database(
        config_dict,
):
    #
    # run task and return the data produced
    #
    sql_query_for_candlestick_pull = get_candlestick_pull_query()
        
    pdf = pull_candlesticks_into_pandas_dataframe(db_connection_str, sql_query_for_candlestick_pull)
    pdf.index = pdf['timestamp'] # move this to the module
        
    full_output_path = save_candlesticks_pandas_dataframe(pdf, pipeline_home)
        
    config_dict['initial_candlesticks_pdf'] = pdf
        
    return config_dict
