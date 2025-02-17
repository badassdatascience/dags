import pendulum
from airflow.decorators import dag, task



# temp
debug_mode = True
run_id = '9754759d-2884-4612-8f32-35e6687b7a16'
run_dir = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/queries'

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

        from pyspark import SparkConf
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as f
        from pyspark.sql.types import ArrayType, IntegerType #, FloatType
        
        # should be in a config file
        keep = ['original_date_shifted', 'timestamp', 'Return', 'Volatility', 'lhc_mean', 'volume']
        seconds_divisor = 60.

        
        full_output_path = str(final_pandas_dict['pandas_preparation_completed_full_output_path']).replace(table_prefix, table_prefix_new)        


        #
        # move this to a config file
        #
        spark_config = SparkConf().setAll(
            [
                ('spark.executor.memory', '15g'),
                ('spark.executor.cores', '3'),
                ('spark.cores.max', '3'),
                ('spark.driver.memory', '15g'),
                ('spark.sql.execution.arrow.pyspark.enabled', 'true'),
            ]
        )

        #
        # define spark session
        #
        spark = (
            SparkSession
            .builder
            .master('local[*]')
            .appName('forex_prep')
            .config(conf = spark_config)
            .getOrCreate()
        )


        if not debug_mode:

            pdf = final_pandas_dict['pandas_preparation_completed_pdf']

            #
            # define a UDF
            #
            udf_difference_an_array = f.udf(difference_an_array, ArrayType(IntegerType()))

            #
            # convert Pandas dataframe to a Spark dataframe
            #
            sdf = (
                spark.createDataFrame(pdf)
                .select(keep)
                .withColumnRenamed('original_date_shifted', 'date_post_shift')
            )

            #
            # for debugging only
            #
            #sdf = sdf.limit(5)
            sdf.show(3)

            #
            #
            #
            sdf_arrays = (
                sdf
                .orderBy('timestamp')
                .groupBy('date_post_shift')
                .agg(
                    f.collect_list('timestamp').alias('timestamp_array'),
                    f.collect_list('Return').alias('return_array'),
                    f.collect_list('Volatility').alias('volatility_array'),
                    f.collect_list('lhc_mean').alias('lhc_mean_array'),
                    f.collect_list('volume').alias('volume_array'),
                )
                .withColumn('seconds_divisor', f.lit(seconds_divisor))
                .withColumn('diff_timestamp', udf_difference_an_array(f.col('timestamp_array'), f.col('seconds_divisor')))
                .drop('seconds_divisor')
                .orderBy('date_post_shift')
            )

            # write to disk
            sdf_arrays.write.mode('overwrite').parquet(full_output_path)

        else:
            sdf_arrays = spark.read.parquet(full_output_path)

            # temp
            #sdf_arrays = sdf_arrays.limit(5)

        
        to_return = {'sdf_arrays_full_output_path' : full_output_path}
        return to_return

    @task()
    def deal_with_nans():

        from pyspark import SparkConf
        from pyspark.sql import SparkSession
        import pyspark.sql.functions as f
        
        #
        # move this to a config file
        #
        spark_config = SparkConf().setAll(
            [
                ('spark.executor.memory', '15g'),
                ('spark.executor.cores', '3'),
                ('spark.cores.max', '3'),
                ('spark.driver.memory', '15g'),
                ('spark.sql.execution.arrow.pyspark.enabled', 'true'),
            ]
        )

        #
        # define spark session
        #
        spark = (
            SparkSession
            .builder
            .master('local[*]')
            .appName('forex_prep')
            .config(conf = spark_config)
            .getOrCreate()
        )
        
        full_output_path = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/queries/spark_9754759d-2884-4612-8f32-35e6687b7a16.parquet'
        sdf_arrays = spark.read.parquet(full_output_path)

        sdf_arrays = sdf_arrays.limit(5)

        import utilities.deal_with_nans as dwn
        sdf_arrays = dwn.deal_with_nans(sdf_arrays)

        sdf_arrays.show(2)

        #
        # test
        #
        # sdf_arrays = (
        #     sdf_arrays
        #     .withColumn('len_ts', f.array_size(f.col('timestamps_all')))
        #     .withColumn('len_return', f.array_size(f.col('return_and_nans')))
        #     .withColumn('len_volat', f.array_size(f.col('volatility_and_nans')))
        #     .withColumn('len_volum', f.array_size(f.col('volume_and_nans')))
        #     .withColumn('len_lhc', f.array_size(f.col('lhc_mean_and_nans')))
        # )

        # sdf_arrays.show(2)
        
        return {'words' : 'words'}
        
    
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

        print(deal_with_nans())

        
        moved_to_spark_dict = {
            'sdf_arrays_full_output_path': run_dir + '/spark_' + run_id + '.parquet',
        }

    


        
#
# declare a dag object
#
dag = PrepareForexData()

#
# main function (for testing the dag object)
#
if __name__ == '__main__':
    dag.test()



