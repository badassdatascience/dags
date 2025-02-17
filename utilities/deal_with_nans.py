
import numpy as np
import pandas as pd

import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType, IntegerType


seconds_divisor = 60



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
        )

    for item in items_list:
        sdf = sdf.drop(item + '_array')

    sdf = sdf.drop('timestamp_array', 'diff_timestamp')

    return sdf

