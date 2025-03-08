
config = {
    'database_name' : 'django',
    'dag_id' : 'NEW_prepare_forex_data',

    'tz_name' : 'US/Eastern',  # DON'T change this!
    
    'price_type_name' : 'mid',
    'instrument_name' : 'EUR/USD',
    'interval_name' : 'Minute',

    'retries_pull_forex_data' : 1,
    'retry_delay_minutes_pull_forex_data' : 5,

    'directory_output' : '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/TEMP',
    'filename_candlesticks_query_results' : 'candlesticks_query_results.parquet',
    'filename_timezone_added' : 'candlesticks_timezone_added.parquet',
}
