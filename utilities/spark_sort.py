import numpy as np

import pyspark.sql.functions as f
from pyspark.sql.types import BooleanType, IntegerType, ArrayType, FloatType

def calculate_order(timestamp_list):
    tl = np.array(timestamp_list)
    to_return = [int(x) for x in np.argsort(tl)]
    return to_return

udf_calculate_order = f.udf(calculate_order, ArrayType(IntegerType()))

def ensure_sort_float(timestamp_sort_list, values_list):
    ts = np.array(timestamp_sort_list)
    v = np.array(values_list)
    to_return = [float(x) for x in v[ts]]
    return to_return

udf_ensure_sort_float = f.udf(ensure_sort_float, ArrayType(FloatType()))

def ensure_sort_int(timestamp_sort_list, values_list):
    ts = np.array(timestamp_sort_list)
    v = np.array(values_list)
    to_return = [int(x) for x in v[ts]]
    return to_return

udf_ensure_sort_int = f.udf(ensure_sort_int, ArrayType(IntegerType()))


#
# need to refactor this a bit
#
def ensure_sort(df):

    df = df.withColumn('timestamps_sorted', udf_calculate_order(f.col('timestamps_all')))

    item_list = ['timestamps_all', 'return', 'volatility', 'volume', 'lhc_mean', 'sin', 'cos']
    for item in item_list:
        if item == 'timestamps_all':
            df = df.withColumn(item + '_sorted', udf_ensure_sort_int(f.col('timestamps_sorted'), f.col(item)))
        else:
            df = df.withColumn(item + '_sorted', udf_ensure_sort_float(f.col('timestamps_sorted'), f.col(item + '_forward_filled')))


    item_list = ['return', 'volatility', 'volume', 'lhc_mean', 'sin', 'cos']
    for item in item_list:
        df = df.drop(item + '_forward_filled')

    df = (
        df
        .drop('timestamps_sorted', 'timestamps_all')
    )

    return df

