import numpy as np
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType


n_back = 180
n_forward = 30

def get_X(the_array):
    return the_array[0:n_back]

def get_y(the_array):
    return the_array[n_back:(n_back + n_forward)]

udf_get_X = f.udf(get_X, ArrayType(FloatType()))
udf_get_y = f.udf(get_y, ArrayType(FloatType()))

def mean_it(the_array):
    return float(np.mean(the_array))

def std_it(the_array):
    return float(np.std(the_array))

udf_mean_it = f.udf(mean_it, FloatType())
udf_std_it = f.udf(std_it, FloatType())

def scale_it(the_array, the_mean, the_std):
    result = (np.array(the_array) - the_mean) / the_std
    return [float(x) for x in result]

udf_scale_it = f.udf(scale_it, ArrayType(FloatType()))


