
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType

# https://numpy.org/devdocs/reference/generated/numpy.lib.stride_tricks.sliding_window_view.html
def make_sliding_window(
        values_array,
        n_back = 180,
        n_forward = 30,
        #n_step = 20,
):
    arr = np.array(values_array)
    swv = sliding_window_view(arr, n_back + n_forward)

    to_return = []
    for i in range(0, swv.shape[0]):
        to_return.append([float(x) for x in swv[i]])
    
    return to_return

udf_make_sliding_window = f.udf(
    make_sliding_window,
    ArrayType(ArrayType(FloatType()))
)
