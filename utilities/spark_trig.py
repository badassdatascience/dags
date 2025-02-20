import numpy as np
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType

seasonal_forecasting_period = 60. * 60. * 24.
seasonal_forecasting_frequency = (2. * np.pi) / seasonal_forecasting_period
seasonal_forecasting_amplitude = 1.

def sin_24_hours(timestamp_array_in_seconds):
    result = seasonal_forecasting_amplitude * np.sin(seasonal_forecasting_frequency * np.array(timestamp_array_in_seconds))
    return [float(x) for x in result]

udf_sin_24_hours = f.udf(sin_24_hours, ArrayType(FloatType()))

def cos_24_hours(timestamp_array_in_seconds):
    result = seasonal_forecasting_amplitude * np.cos(seasonal_forecasting_frequency * np.array(timestamp_array_in_seconds))
    return [float(x) for x in result]

udf_cos_24_hours = f.udf(cos_24_hours, ArrayType(FloatType()))

def add_trig_functions_spark(
        df,
        postfix = 'forward_filled',
        ts_col = 'timestamps_all'
):
    df = (
        df
        .withColumn('sin_' + postfix, udf_sin_24_hours(f.col(ts_col)))
        .withColumn('cos_' + postfix, udf_cos_24_hours(f.col(ts_col)))
    )
    return df

