
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType, IntegerType

n_back = 180
n_forward = 30
# n_step = 20
offset = 1


# https://numpy.org/devdocs/reference/generated/numpy.lib.stride_tricks.sliding_window_view.html
def make_sliding_window_float(
        values_array,
        n_back = n_back,
        n_forward = n_forward,
        #n_step = n_step,
):
    arr = np.array(values_array)
    swv = sliding_window_view(arr, n_back + n_forward)

    to_return = []
    for i in range(0, swv.shape[0]):
        to_return.append([float(x) for x in swv[i]])
    
    return to_return

def make_sliding_window_int(
        values_array,
        n_back = n_back,
        n_forward = n_forward,
        #n_step = n_step,
):
    arr = np.array(values_array)
    swv = sliding_window_view(arr, n_back + n_forward)

    to_return = []
    for i in range(0, swv.shape[0]):
        to_return.append([int(x) for x in swv[i]])
    
    return to_return


udf_make_sliding_window_float = f.udf(
    make_sliding_window_float,
    ArrayType(ArrayType(FloatType()))
)

udf_make_sliding_window_int = f.udf(
    make_sliding_window_int,
    ArrayType(ArrayType(IntegerType()))
)

def find_too_short(
        df,
        n_forward = n_forward,
        n_back = n_back,
        offset = offset,
        column_name = 'timestamps_all_sorted_length',
):
    return df.where(f.col(column_name) >= (n_back + n_forward + offset))
