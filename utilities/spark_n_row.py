
from pyspark.sql import functions as f
from pyspark.sql import Window as w

def get_nth_rows(
        df,
        columns = ['timestamp_first'],
        n_step = 200,
):

    window = w.orderBy(columns)
    to_return = (
        df
        .withColumn('row_number', f.row_number().over(window))
        .filter(f.col('row_number') % n_step == 0)
        #.drop('row_number')
    )
    return to_return
