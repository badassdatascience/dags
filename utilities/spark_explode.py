import pyspark.sql.functions as f

def spark_explode_it(df):

    to_return = (
        df
        .withColumn('zipped_array', f.arrays_zip('sw_timestamps', 'sw_return', 'sw_volatility', 'sw_volume', 'sw_lhc_mean', 'sw_sin', 'sw_cos'))  # timestamps_all_sorted
        .withColumn('zipped_array', f.explode('zipped_array'))
        .select(
            'date_post_shift',
            #f.col('zipped_array.timestamps_all_sorted').alias('timestamps_start'),
            f.col('zipped_array.sw_timestamps').alias('timestamps'),
            f.col('zipped_array.sw_return').alias('return'),
            f.col('zipped_array.sw_volatility').alias('volatility'),
            f.col('zipped_array.sw_volume').alias('volume'),
            f.col('zipped_array.sw_lhc_mean').alias('lhc_mean'),
            f.col('zipped_array.sw_sin').alias('sin'),
            f.col('zipped_array.sw_cos').alias('cos'),
        )
    )

    return to_return

