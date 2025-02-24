import pendulum
from airflow.decorators import dag, task



# temp
debug_mode = True
limit_5 = False
run_id = '309457bc-a227-4332-8c0b-2cf5dd38749c'
run_dir = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/queries'
n_processors = 20



#
# not sure this is the best place
#
def difference_an_array(the_array, seconds_divisor):
    return [int((y - x) / seconds_divisor) for x, y in zip(the_array[0:-1], the_array[1:])]






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

        # Should this be here or somewhere else?
        pdf = pdf[~pdf['weekday_shifted'].isna()]

        full_output_path = str(merged_candlesticks_dict['merged_candlesticks_pdf_full_output_path']).replace(table_prefix, table_prefix_new)

        pdf.to_parquet(full_output_path)
        
        to_return = {'shifted_candlesticks_pdf' : pdf, 'shifted_candlesticks_pdf_full_output_path' : full_output_path}        
    
        return to_return

    @task()
    def finalize_pandas_dataframe(
            shifted_candlesticks_dict,
            table_prefix = 'shifted',
            table_prefix_new = 'pandas_preparation_completed',
    ):
        import numpy as np
        
        pdf = shifted_candlesticks_dict['shifted_candlesticks_pdf']
        pdf['Return'] = pdf['c'] - pdf['o']
        pdf['Volatility'] = pdf['h'] - pdf['l']
        pdf['lhc_mean'] = pdf[['l', 'h', 'c']].mean(axis = 1, skipna = True)
        
        full_output_path = str(shifted_candlesticks_dict['shifted_candlesticks_pdf_full_output_path']).replace(table_prefix, table_prefix_new)        

        pdf.to_parquet(full_output_path)
        
        to_return = {table_prefix_new + '_pdf' : pdf, table_prefix_new + '_full_output_path' : full_output_path}

        return to_return



    @task()
    def move_to_spark(
            final_pandas_dict,
            table_prefix = 'pandas_preparation_completed',
            table_prefix_new = 'spark',
    ):


    ######################
    #   Deal with NaNs   #
    ######################
    
    @task()
    def deal_with_nans():

        import numpy as np
        import pyspark.sql.functions as f
        from pyspark.sql.types import BooleanType, IntegerType, ArrayType, FloatType
        

        ###########################
        #   Get a Spark session   #
        ###########################

        # this MAY only be necessary for debugging... not sure yet
        
        from utilities.spark_session import get_spark_session
        spark = get_spark_session()
        spark.catalog.clearCache()  # will this help?

        
        ##########################################
        #   Load the data (temporary debugging)  #
        ##########################################
        
        full_output_path = run_dir + '/spark_' + run_id + '.parquet'
        sdf_arrays = spark.read.parquet(full_output_path)

        print()
        sdf_arrays.show(2)
        print()
        print(sdf_arrays.count())
        print()

        ############
        #   Test   #
        ############

        sdf_arrays = sdf_arrays.coalesce(n_processors)
        
        #####################
        #   For debugging   #
        #####################

        if limit_5:
            sdf_arrays = sdf_arrays.limit(5)

        
        ######################
        #   Deal with NaNs   #
        ######################
        
        import utilities.spark_deal_with_nans as dwn
        sdf_arrays = dwn.deal_with_nans(sdf_arrays)

        
        ##########################
        #   Add trig functions   #
        ##########################
        
        from utilities.spark_trig import add_trig_functions_spark
        sdf_arrays = add_trig_functions_spark(sdf_arrays)

        
        ###############################
        #   Ensure order is correct   #
        ###############################

        from utilities.spark_sort import ensure_sort
        sdf_arrays = ensure_sort(sdf_arrays)

        
        ################################
        #   QA before sliding window   #
        ################################
        
        item_ff_list = ['timestamps_all_sorted', 'return', 'volatility', 'volume', 'lhc_mean', 'sin', 'cos']
        for item in item_ff_list:
            if item == 'timestamps_all_sorted':
                sdf_arrays = sdf_arrays.withColumn('timestamps_all_sorted_length', f.array_size(f.col('timestamps_all_sorted')))
            else:
                sdf_arrays = sdf_arrays.withColumn(item + '_length', f.array_size(f.col(item + '_sorted')))
                
        def array_length_test(timestamps_array, return_array, volatility_array, volume_array, lhc_mean_array, sin_array, cos_array):
            length_mean = np.mean(
                (
                    len(timestamps_array),
                    len(return_array),
                    len(volatility_array),
                    len(volume_array),
                    len(lhc_mean_array),
                    len(sin_array),
                    len(cos_array),
                )
            )
            return int(length_mean) == len(timestamps_array)

        udf_array_length_test = f.udf(array_length_test, BooleanType())

        sdf_arrays = (
            sdf_arrays
            .withColumn(
                'array_length_mean_test',
                udf_array_length_test(
                    f.col('timestamps_all_sorted'),
                    f.col('return_sorted'),
                    f.col('volatility_sorted'),
                    f.col('volume_sorted'),
                    f.col('lhc_mean_sorted'),
                    f.col('sin_sorted'),
                    f.col('cos_sorted'),
                )
            )
            .where(f.col('array_length_mean_test') == True)
        )

        item_list = ['return', 'volatility', 'volume', 'lhc_mean', 'sin', 'cos']
        for item in item_list:
            sdf_arrays = sdf_arrays.drop(item + '_length')

        sdf_arrays = sdf_arrays.drop('array_length_mean_test')
                        
        sdf_arrays.show(2)


        print()
        (
            sdf_arrays
            .agg(
                f.min('timestamps_all_sorted_length').alias('the_min'),
                f.max('timestamps_all_sorted_length').alias('the_max'),
            )
            .show(10)
        )
        print()
        

        sdf_arrays.show(2)
        print()

        ############
        #   Test   #
        ############

        sdf_arrays = sdf_arrays.coalesce(n_processors)
        
        ###############################################
        #   Find lists too short for sliding window   #
        ###############################################

        # I don't know YET why there is only one row that is two short

        from utilities.spark_sliding_window import find_too_short
        sdf_arrays = find_too_short(sdf_arrays)
        sdf_arrays.show(2)
        
        # # are there NULLs at this point?
        # print()
        # sdf_arrays.where(f.col('timestamps_all_sorted_length') <= 300).show()
        # print()
        
        
        ######################
        #   Sliding window   #
        ######################
        
        from utilities.spark_sliding_window import do_sliding_window
        sdf_arrays = (
            do_sliding_window(sdf_arrays)
            .dropna()
        )
        sdf_arrays.show(2)

       
        ######################
        #   Explode arrays   #
        ######################

        from utilities.spark_explode import spark_explode_it
        sdf_arrays = spark_explode_it(sdf_arrays)
       
       
        ############
        #   Test   #
        ############

        sdf_arrays = sdf_arrays.coalesce(n_processors)


        
        #######################################################
        #   QA:  Figure out where the NULLs are coming from   #
        #######################################################

        #
        # we are also measuring array lengths in this code...
        # ...so refactor this baby
        #



        
        
        items_list = ['return', 'volatility', 'volume', 'lhc_mean', 'sin', 'cos']
        for item in items_list:
            sdf_arrays = sdf_arrays.withColumn('size_' + item, f.array_size(f.col(item)))
        sdf_arrays = sdf_arrays.withColumn('size_timestamps', f.array_size(f.col('timestamps')))

        # I might repeat this elsewhere. Check later...
        def test_length_2(len_timestamps, len_returns, len_volatility, len_volume, len_lhc_mean, len_sine, len_cosine):
            to_return = (
                (len_timestamps == len_returns) &
                (len_returns == len_volatility) &
                (len_volatility == len_volume) &
                (len_volume == len_lhc_mean) &
                (len_lhc_mean == len_sine) &
                (len_sine == len_cosine)
            )
            return to_return

        udf_test_length_2 = f.udf(test_length_2, BooleanType())
       
        sdf_arrays = (
            sdf_arrays
            .withColumn(
                'column_length_status',
                udf_test_length_2(
                    f.col('size_timestamps'),
                    f.col('size_return'),
                    f.col('size_volatility'),
                    f.col('size_volume'),
                    f.col('size_lhc_mean'),
                    f.col('size_sin'),
                    f.col('size_cos'),
                )
            )
        )

        sdf_arrays = (
            sdf_arrays
            .where(f.col('column_length_status') == True)
            .drop('column_length_status')
            .drop('size_return', 'size_volatility', 'size_volume', 'size_lhc_mean', 'size_sin', 'size_cos')
            .withColumn('timestamp_first', f.col('timestamps')[0])
        )





        
        #
        # This prompts an error
        #
        # print()
        # (
        #     sdf_arrays
        #     .groupBy('size_timestamps')
        #     .agg(
        #         f.count('size_timestamps').alias('count')
        #     )
        # ).show(10)
        # print()



        
        # (
        #     sdf_arrays.select('size_timestamps').distinct().show(10)
        # )




        
        # #
        # # Where are these NULLs coming from?
        # #
        # print()
        # sdf_arrays.where(f.col('size_timestamps').isNull()).show(10)
        # print()


        




        
        # # this is not giving clear results
        # (
        #     sdf_arrays
        #     .withColumn('not_null', sdf_arrays['timestamps'].isNotNull())
        #     .groupBy('not_null')
        #     .agg(f.count('timestamps').alias('count'))
        #     .show(10)
        # )
        # print()

        
        # #
        # # Save the version with NULLs for later investigation
        # #
        # full_exploded_with_NULLs_output_path = run_dir + '/spark_exploded_with_NULLs_' + run_id
        # sdf_arrays.write.mode('overwrite').parquet(full_exploded_with_NULLs_output_path)
        
        #
        # Remove the NULLs for now (and investigate why there are NULLs in the near future...)
        #
        sdf_arrays = (
            sdf_arrays
            .withColumn('not_null', sdf_arrays['timestamps'].isNotNull())
            .where(f.col('not_null') == True)
            .drop('not_null')
            .dropna()
        )
        
        #######


        
        
        ############
        #   Test   #
        ############

        sdf_arrays = sdf_arrays.coalesce(n_processors)


        ######################
        #   Final clean up   #
        ######################

        sdf_arrays = sdf_arrays.drop('timestamps', 'size_timestamps').dropna()


        # #####################
        # #   Get Nth items   #
        # #####################

        # from utilities.spark_n_row import get_nth_rows
        # spark.conf.set("spark.sql.shuffle.partitions", 10)  # temp
        
        # sdf_arrays = get_nth_rows(sdf_arrays)

        # print()
        # sdf_arrays.show(10)
        # print()
        
        
        ####################
        #   Save results   #
        ####################

        # Test
        sdf_arrays = sdf_arrays.coalesce(n_processors)
        
        full_exploded_output_path = run_dir + '/spark_exploded_' + run_id + '.parquet'
        sdf_arrays.write.mode('overwrite').parquet(full_exploded_output_path)

        
        ##############
        #   Return   #
        ##############

        spark.stop()
        
        to_return = {
            'full_exploded_output_path' : full_exploded_output_path,
        }
        
        return to_return

    # ###############
    # #   X and y   #
    # ###############
    
    # @task()
    # def derive_X_and_y(
    #         #spark_exploded_data_dict : dict,
    # ):

        
    
    #
    # define pipeline component order and dependencies
    #
    if not debug_mode:
        candlestick_data_dict = extract_candlestick_data_from_database()
        candlestick_data_timezone_dict = add_timezone_information_to_original_pull(candlestick_data_dict)
        offset_map_dict = generate_weekday_hour_offset_mapping(candlestick_data_timezone_dict)
        merged_dict = merge_timezone_shift(candlestick_data_timezone_dict, offset_map_dict)
        shifted_dict = shift_days_and_hours_as_needed(merged_dict)
        final_pandas_dict = finalize_pandas_dataframe(shifted_dict)
        moved_to_spark_dict = move_to_spark(final_pandas_dict)

    else:
        #
        # debugging
        #

        # What arguments go here? A filepath?
        nans_dict = deal_with_nans()  

        #X_y_dict = derive_X_and_y()        

        
        #print()
        #import pprint as pp
        #pp.pprint(X_y_dict)
        #print()
        
    


        
#
# declare a dag object
#
dag = PrepareForexData()

#
# main function (for testing the dag object)
#
if __name__ == '__main__':
    dag.test()



