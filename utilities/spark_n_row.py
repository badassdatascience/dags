
from pyspark.sql import functions as f
from pyspark.sql import Window as w

def get_nth_rows(
        df,
        group_by_column = 'timestamp_first',
        partition_by_column = 'date_post_shift',
        n_step = 200,
):
    window = w.partitionBy(partition_by_column).orderBy(group_by_column)
    to_return = (
        df
        .withColumn('row_number', f.row_number().over(window))
        .filter(f.col('row_number') % n_step == 0)
        .drop('row_number')
    )
    return to_return
