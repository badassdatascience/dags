
import numpy as np
import pandas as pd

import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType, IntegerType


seconds_divisor = 60


#https://stackoverflow.com/questions/41190852/most-efficient-way-to-forward-fill-nan-values-in-numpy-array
def do_nans_exist(values_array):
    values_array = np.array([np.array(values_array)])
    mask = np.isnan(values_array)
    has_nan_0_or_1 = np.max([int(x) for x in mask[0]])
    return int(has_nan_0_or_1)

def do_non_nans_exist(values_array):
    values_array = np.array([np.array(values_array)])
    mask = ~np.isnan(values_array)
    has_nan_0_or_1 = np.max([int(x) for x in mask[0]])
    return int(has_nan_0_or_1)

udf_do_nans_exist = f.udf(do_nans_exist, IntegerType())
udf_do_non_nans_exist = f.udf(do_nans_exist, IntegerType())

def get_all_timestamps(timestamp_array, seconds_divisor):
    return [int(x) for x in range(min(timestamp_array), max(timestamp_array) + seconds_divisor, seconds_divisor)]

udf_get_all_timestamps = f.udf(get_all_timestamps, ArrayType(IntegerType()))



def locate_nans(timestamp_array, timestamp_all_array, values_array):

    # make sure we get an argsort in here later to ensure order of values is correct

    ts = np.array(timestamp_array, dtype = np.uint64) # ??
    ts_all = np.array(timestamp_all_array, dtype = np.uint64)  # we can probably make this smaller
    v = np.array(values_array, dtype = np.float64)  # we can probably make this smaller
    
    pdf = pd.DataFrame({'timestamp' : ts, 'values' : v})
    pdf_all = pd.DataFrame({'timestamp' : ts_all})

    pdf_joined = (
        pd.merge(
            pdf_all,
            pdf,
            on = 'timestamp',
            how = 'left',
        )
    )

    to_return = pdf_joined['values'].to_list()
    
    return to_return

udf_locate_nans = f.udf(locate_nans, ArrayType(FloatType()))




def deal_with_nans(sdf):

    sdf = (
        sdf
        .withColumn(
            'timestamps_all',
        udf_get_all_timestamps(f.col('timestamp_array'), f.lit(seconds_divisor))
        )
    )

    items_list = ['return', 'volatility', 'volume', 'lhc_mean']

    for item in items_list:
        sdf = (
            sdf
            .withColumn(
                item + '_and_nans',
                udf_locate_nans(f.col('timestamp_array'), f.col('timestamps_all'), f.col(item + '_array'))
            )

            #
            # useful for testing
            #
            # .withColumn(
            #     item + '_nans_yes',
            #     udf_do_nans_exist(f.col(item + '_and_nans'))
            # )
            # .withColumn(
            #     item + '_non_nans_yes',
            #     udf_do_non_nans_exist(f.col(item + '_and_nans'))
            # )
            
        )

    for item in items_list:
        sdf = sdf.drop(item + '_array')

    sdf = sdf.drop('timestamp_array', 'diff_timestamp')

    return sdf

