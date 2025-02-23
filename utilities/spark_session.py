

from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_spark_session():

    #
    # move this to a config file
    #
    spark_config = SparkConf().setAll(
        [
            ('spark.executor.memory', '75g'),
            ('spark.executor.cores', '20'),
            ('spark.cores.max', '20'),
            ('spark.driver.memory', '75g'),
            ('spark.sql.execution.arrow.pyspark.enabled', 'true'),
        ]
    )

    #
    # define spark session
    #
    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .appName('forex_prep')
        .config(conf = spark_config)
        .getOrCreate()
    )

    return spark
