
def task_extract_candlestick_data_from_database():

    #
    # TEMP until I figure out how to do this in airflow
    #
    pipeline_home = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components'
    import sys;
    sys.path.append(pipeline_home)

    #
    # load the libraries we need
    #
    from get_database_connection_string import db_connection_str
    from get_sql_for_pull import get_candlestick_pull_query
    from pull_data_from_database import pull_candlesticks_into_pandas_dataframe
    from pull_data_from_database import save_candlesticks_pandas_dataframe

    #
    # run task and return the data produced
    #
    if not debug_mode:
        sql_query_for_candlestick_pull = get_candlestick_pull_query()
        
        pdf = pull_candlesticks_into_pandas_dataframe(db_connection_str, sql_query_for_candlestick_pull)  # .sort_values(by = ['timestamp']) # move the sort procedure to the module

        pdf.index = pdf['timestamp'] # move this to the module
        
        full_output_path = save_candlesticks_pandas_dataframe(pdf, pipeline_home)
        
        to_return = {'initial_candlesticks_pdf' : pdf, 'initial_candlesticks_pdf_full_output_path' : full_output_path}
        
    else:
        import pandas as pd
        test_file = run_dir + '/candlestick_query_results_' + run_id + '.parquet'
        pdf = pd.read_parquet(test_file)
        
        to_return = {'initial_candlesticks_pdf' : pdf, 'initial_candlesticks_pdf_full_output_path' : test_file}
        
    return to_return
