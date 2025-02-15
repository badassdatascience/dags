import pendulum
from airflow.decorators import dag, task

# temp
debug_mode = True
run_id = '1f09ecbb-2d83-46c6-9e9c-195792519cb6'
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
    def merge_timezone_shift(
            candlestick_data_timezone_dict : dict,
            offset_map_dict : dict,
            table_prefix = 'timezone_added', # get this somewhere else
            table_prefix_new = 'merge_by_pandas',
    ):
        """
        Meh.
        """

        import pandas as pd
        pdf = (
            pd.merge(
                candlestick_data_timezone_dict['tz_added_candlesticks_pdf'],
                offset_map_dict['shifted_candlesticks_pdf'],
                on = ['weekday_tz', 'hour_tz'],
                how = 'left'
            )
            .sort_values(by = ['datetime_tz'])
        )

        full_output_path = str(candlestick_data_timezone_dict['tz_added_candlesticks_pdf_full_output_path']).replace(table_prefix, table_prefix_new)

        pdf.to_parquet(full_output_path)
        
        to_return = {'merged_candlesticks_pdf' : pdf, 'merged_candlesticks_pdf_full_output_path' : full_output_path}        

        return to_return



    @task()
    def shift_days_and_hours_as_needed(
            merged_candlesticks_dict,
            table_prefix = 'merge_by_pandas',
            table_prefix_new = 'shifted',
    ):

        import datetime
        import pandas as pd

        df = merged_candlesticks_dict['merged_candlesticks_pdf']
    
        df['original_date'] = [x.date() for x in df['datetime_tz']]
        df['to_shift'] = df['weekday_shifted'] - df['weekday_tz']

        pdf_date_to_shift = (
            df
            .sort_values(by = 'datetime_tz')
            [['weekday_tz', 'hour_tz', 'weekday_shifted', 'original_date', 'to_shift']]
            .drop_duplicates()
        )

        new_date_list = []
        for i, row in pdf_date_to_shift.iterrows():
            if row['to_shift'] > 0:
                delta = datetime.timedelta(days = row['to_shift'])
                new_date_list.append(row['original_date'] + delta)
            elif row['to_shift'] == -6:
                delta = datetime.timedelta(days = 1)
                new_date_list.append(row['original_date'] + delta)
            else:
                new_date_list.append(row['original_date'])

        pdf_date_to_shift['original_date_shifted'] = new_date_list

        pdf = (
            pd.merge(
                df.drop(columns = ['to_shift']),
                pdf_date_to_shift,
                on = ['weekday_tz', 'hour_tz', 'weekday_shifted', 'original_date'],
                how = 'left',
            )
            .drop(columns = ['original_date', 'to_shift'])
            .sort_values(by = ['datetime_tz'])
        )

        full_output_path = str(merged_candlesticks_dict['merged_candlesticks_pdf_full_output_path']).replace(table_prefix, table_prefix_new)

        pdf.to_parquet(full_output_path)
        
        to_return = {'shifted_candlesticks_pdf' : pdf, 'shifted_candlesticks_pdf_full_output_path' : full_output_path}        
    
        return to_return

    #
    # define pipeline component order and dependencies
    #
    candlestick_data_dict = extract_candlestick_data_from_database()
    candlestick_data_timezone_dict = add_timezone_information_to_original_pull(candlestick_data_dict)
    offset_map_dict = generate_weekday_hour_offset_mapping(candlestick_data_timezone_dict)
    merged_dict = merge_timezone_shift(candlestick_data_timezone_dict, offset_map_dict)
    shifted_dict = shift_days_and_hours_as_needed(merged_dict)

#
# declare a dag object
#
dag = PrepareForexData()

#
# main function (for testing the dag object)
#
if __name__ == '__main__':
    dag.test()



