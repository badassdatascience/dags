
import numpy as np

import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType





udf_array_mean = f.udf(lambda x: float(np.mean(x)), FloatType())

udf_array_median = f.udf(lambda x: float(np.median(x)), FloatType())

def assemble_panel_elements(returns, volatility, volume, lhc_mean, sine, cosine):
    to_return = [returns, volatility, volume, lhc_mean, sine, cosine]
    return to_return

udf_assemble_panel_elements = f.udf(assemble_panel_elements, ArrayType(ArrayType(FloatType())))

def build_matrices(last_task_dict):

    from utilities.spark_session import get_spark_session
    spark = get_spark_session()
    spark.catalog.clearCache()  # will this help?


    full_scaled_path = last_task_dict['full_scaled_path']
    sdf_arrays = spark.read.parquet(full_scaled_path)

    if last_task_dict['limit_5']:
        sdf_arrays = sdf_arrays.limit(5)

    #
    # y statistics
    #
    for item in ['return_y_scaled', 'lhc_mean_y_scaled']:
        sdf_arrays = (
            sdf_arrays
            .withColumn(item + '_mean', udf_array_mean(f.col(item)))
            .withColumn(item + '_median', udf_array_median(f.col(item)))
            .withColumn(item + '_min', f.array_min(f.col(item)))
            .withColumn(item + '_max', f.array_max(f.col(item)))
        )

    sdf_arrays = (
        sdf_arrays
        .withColumn(
            'X',
            udf_assemble_panel_elements(
                'return_X_scaled',
                'volatility_X_scaled',
                'volume_X_scaled',
                'lhc_mean_X_scaled',
                'sin_X',
                'cos_X',
            )
        )
        .drop(
            'return_X_scaled',
            'volatility_X_scaled',
            'volume_X_scaled',
            'lhc_mean_X_scaled',
            'sin_X',
            'cos_X',
        )
    )

    #
    # temp
    #
    n_processors = 20


    

    #
    # save
    #
    full_final_path = full_scaled_path.replace('spark_scaled_', 'spark_final_')

    sdf_arrays = (
        sdf_arrays
        .coalesce(n_processors)
        .dropna()
        .write.mode('overwrite')
        .parquet(full_final_path)
    )

    spark.stop()

    return {
        'full_final_path' : full_final_path,
    }
