import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id = 'prepare_forex_data',
    schedule = None,
    start_date = pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup = False,
    tags=['forex', 'mysql', 'time series'],
)
def PrepareForexData():
    """
    This DAG prepares forex data for downstream ML
    """

    @task()
    def extract_candlestick_data_from_database():
        """
        This task extracts candlestick data from the MySQL database
        """

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
        if False:
            sql_query_for_candlestick_pull = get_candlestick_pull_query()

            pdf = pull_candlesticks_into_pandas_dataframe(db_connection_str, sql_query_for_candlestick_pull).sort_values(by = ['timestamp']) # move the sort procedure to the module

            pdf.index = pdf['timestamp'] # move this to the module

            full_output_path = save_candlesticks_pandas_dataframe(pdf, pipeline_home)
        
            to_return = {'initial_candlesticks_pdf' : pdf, 'initial_candlesticks_pdf_full_output_path' : full_output_path}

        else:
            import pandas as pd
            test_file = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/queries/candlestick_query_results_a3aa77b8-0bfe-4fa3-9216-c79db92876e4.parquet'
            pdf = pd.read_parquet(test_file)

            to_return = {'initial_candlesticks_pdf' : pdf, 'initial_candlesticks_pdf_full_output_path' : test_file}

        return to_return

    @task()
    def generate_weekday_hour_offset_mapping(candlestick_data_dict : dict):

        import pandas as pd
        
        # TEMP, get this from somewhere else
        table_prefix = 'candlestick_query_results'
        table_prefix_new = 'weekday_hour_shifted'

        hour_list = []
        weekday_list = []
        shifted_list = []

        shifted_list.extend([0] * 17)

        for i in range(0, 4):
            weekday_list.extend([i] * 24)
            hour_list.extend(sorted(list(range(0, 24))))

            if i >= 1:
                shifted_list.extend([i] * 24)

        shifted_list.extend([4] * 24)

        weekday_list.extend([4] * 17)
        hour_list.extend(sorted(list(range(0, 17))))

        weekday_list.extend([6] * 7)
        hour_list.extend(sorted(list(range(17, 24))))
        shifted_list.extend([0] * 7)

        pdf_shifted_weekday_manually_constructed = pd.DataFrame({'weekday_tz' : weekday_list, 'hour_tz' : hour_list, 'weekday_shifted' : shifted_list})

        filename_and_path = str(candlestick_data_dict['initial_candlesticks_pdf_full_output_path']).replace(table_prefix, table_prefix_new)
        
        pdf_shifted_weekday_manually_constructed.to_parquet(filename_and_path)

        to_return = {'shifted_candlesticks_pdf' : pdf_shifted_weekday_manually_constructed, 'shifted_candlesticks_pdf_full_output_path' : filename_and_path}

        return to_return
        
    #
    # we are not currently using this;
    # it is leftover from the tutorial I used
    # and I'm keeping it for a slight bit longer
    # as a reference
    #
    @task(multiple_outputs = True)
    def placeholder(candlestick_data_dict: dict):
        """
        This task is not defined yet; still trying to 
        get airflow to work correctly before fixing this.
        I.e., it is a placeholder just to ensure we have
        a test pipeline containing a dependency.
        """
        return {'key' : 'words words words'}

    #
    # define pipeline component order and dependencies
    #
    candlestick_data_dict = extract_candlestick_data_from_database()
    generate_weekday_hour_offset_mapping(candlestick_data_dict)
    temporary_placeholder = placeholder(candlestick_data_dict)

#
# declare a dag object
#
dag = PrepareForexData()

#
# main function (for testing the dag object)
#
if __name__ == '__main__':
    dag.test()
