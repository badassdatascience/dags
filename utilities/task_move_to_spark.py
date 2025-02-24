#import pendulum
#from airflow.decorators import dag
from airflow.decorators import task

#from pyspark import SparkConf
#from pyspark.sql import SparkSession
import pyspark.sql.functions as f



from pyspark.sql.types import ArrayType, IntegerType


def task_move_to_spark(


):

    spark = 
        
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
        if limit_5:
            sdf = sdf.limit(5)
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
