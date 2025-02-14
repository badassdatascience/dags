import pendulum
from airflow.decorators import dag, task

# temp
debug_mode = True
run_id = 'b69f9503-6ef9-4e37-8483-16679ed3b8fc'
run_dir = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/queries'

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

    @task()
    def add_timezone_information_to_original_pull(
            candlestick_data_dict : dict,
            tz_name = 'US/Eastern',
            table_prefix = 'candlestick_query_results', # get this somewhere else
            table_prefix_new = 'timezone_added',
    ):

        if not debug_mode:
        
            import pytz
            import datetime

            previous_output_filename = str(candlestick_data_dict['initial_candlesticks_pdf_full_output_path'])
        
            tz = pytz.timezone(tz_name)
        
            pdf = candlestick_data_dict['initial_candlesticks_pdf']
            pdf['datetime_tz'] = [datetime.datetime.fromtimestamp(x, tz) for x in pdf['timestamp']]
            pdf['weekday_tz'] = [datetime.datetime.weekday(x) for x in pdf['datetime_tz']]
            pdf['hour_tz'] = [x.hour for x in pdf['datetime_tz']]

            filename_and_path = previous_output_filename.replace(table_prefix, table_prefix_new)

            pdf.to_parquet(filename_and_path)
        
            to_return = {'tz_added_candlesticks_pdf' : pdf, 'tz_added_candlesticks_pdf_full_output_path' : filename_and_path}

        else:
            import pandas as pd
            test_file = run_dir + '/timezone_added_' + run_id + '.parquet'
            pdf = pd.read_parquet(test_file)
            to_return = {'tz_added_candlesticks_pdf' : pdf, 'tz_added_candlesticks_pdf_full_output_path' : test_file}

        return to_return
        
    @task()
    def generate_weekday_hour_offset_mapping(candlestick_data_timezone_dict : dict):

        #
        # TEMP until I figure out how to do this in airflow
        #
        pipeline_home = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components'
        import sys;
        sys.path.append(pipeline_home)

        #
        # load the libraries we need
        #
        from offset import generate_offset_map

        #
        # compute
        #
        to_return = generate_offset_map(candlestick_data_timezone_dict['tz_added_candlesticks_pdf_full_output_path'])

        return to_return
        
    #
    # we are not currently using this;
    # it is leftover from the tutorial I used
    # and I'm keeping it for a slight bit longer
    # as a reference
    #
    @task(multiple_outputs = True)
    def placeholder(candlestick_data_dict : dict, offset_map_dict : dict):
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
    candlestick_data_timezone_dict = add_timezone_information_to_original_pull(candlestick_data_dict)
    offset_map_dict = generate_weekday_hour_offset_mapping(candlestick_data_timezone_dict)
    temporary_placeholder = placeholder(candlestick_data_timezone_dict, offset_map_dict)

#
# declare a dag object
#
dag = PrepareForexData()

#
# main function (for testing the dag object)
#
if __name__ == '__main__':
    dag.test()
