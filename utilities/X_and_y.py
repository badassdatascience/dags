    #     import numpy as np
    #     import pyspark.sql.functions as f
    #     from pyspark.sql.types import BooleanType, IntegerType, ArrayType, FloatType

    #     # this MAY only be necessary for debugging... not sure yet
    #     from utilities.spark_session import get_spark_session
    #     spark = get_spark_session()
    #     spark.catalog.clearCache()  # will this help?

    #     full_exploded_output_path = run_dir + '/spark_exploded_' + run_id + '.parquet'
    #     sdf_arrays = (
    #         spark
    #         .read
    #         .parquet(full_exploded_output_path)
    #         .orderBy('date_post_shift', 'timestamp_first')
    #     )
    #     sdf_arrays.show(5)
            

    #     def X_it(array):
    #         n_back = 180
    #         X = array[0:n_back]
    #         return X

    #     udf_X_it = f.udf(X_it, ArrayType(FloatType()))

    #     def y_it(array):
    #         n_back = 180
    #         n_forward = 30
    #         y = array[n_back:(n_back + n_forward)]
    #         return y

    #     udf_y_it = f.udf(y_it, ArrayType(FloatType()))

    #     item_list = ['return', 'volatility', 'volume', 'lhc_mean', 'sin', 'cos']
    #     for item in item_list:
    #         sdf_arrays = (
    #             sdf_arrays
    #             .withColumn(item + '_X', udf_X_it(item))
    #             .withColumn(item + '_y', udf_y_it(item))
    #             .drop(item)
    #         )

    #     sdf_arrays = sdf_arrays.drop('size_timestamps')
            
    #     def stack_it(returns, volatility, volume, lhc_mean, sin, cos):
    #         M = [
    #             returns,
    #             volatility,
    #             volume,
    #             lhc_mean,
    #             sin,
    #             cos,
    #         ]
    #         return M
            
    #     udf_stack_it = f.udf(stack_it, ArrayType(ArrayType(FloatType())))
        
    #     sdf_arrays = (
    #         sdf_arrays
    #         .withColumn('X', udf_stack_it('return_X', 'volatility_X', 'volume_X', 'lhc_mean_X', 'sin_X', 'cos_X'))
    #         .drop('return_X', 'volatility_X', 'volume_X', 'lhc_mean_X', 'sin_X', 'cos_X')
    #     )

    #     sdf_arrays = (
    #         sdf_arrays.drop('volatility_y', 'volume_y', 'sin_y', 'cos_y')
    #     )

    #     # # y
    #     # sdf_arrays = (
    #     #     sdf_arrays
    #     #     #.withColumn('return_y_mean', f.mean(f.col('return_y')))
    #     #     #.withColumn('lhc_mean_y_mean', f.mean(f.col('lhc_mean_y')))
    #     #     .withColumn('return_y_min', f.array_min(f.col('return_y')))
    #     #     .withColumn('lhc_mean_y_min', f.array_min(f.col('lhc_mean_y')))
    #     #     .withColumn('return_y_max', f.array_max(f.col('return_y')))
    #     #     .withColumn('lhc_mean_y_max', f.array_max(f.col('lhc_mean_y')))
    #     #     #.withColumn('return_y_median', f.median(f.col('return_y')))
    #     #     #.withColumn('lhc_mean_y_median', f.median(f.col('lhc_mean_y')))
    #     # )

    #     sdf_arrays.show(3)   
            
    #     #
    #     # to Pandas
    #     #
    #     pdf = sdf_arrays.toPandas()

    #     print()
    #     print(pdf['X'].to_numpy().shape)
    #     print()

        

        


    #     return {'booger' : 'booger'}
