
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime, timedelta



def pull_forex_data(
        price_type_name = 'mid',
        instrument_name = 'EUR/USD',
        interval_name = 'Minute',
        output_directory = None,
):
    mysql_hook = MySqlHook(mysql_conn_id = 'django')

    sql = f"SELECT ts.timestamp, cs.o, cs.l, cs.h, cs.c, v.volume FROM timeseries_candlestick cs, timeseries_instrument inst, timeseries_interval iv, timeseries_pricetype pt, timeseries_volume v, timeseries_timestamp ts WHERE cs.instrument_id = inst.id AND cs.interval_id = iv.id AND cs.price_type_id = pt.id AND cs.volume_id = v.id AND cs.timestamp_id = ts.id AND pt.name = '%s' AND inst.name = '%s' AND iv.name = '%s' ORDER BY timestamp;"

    pdf = mysql_hook.get_pandas_df(
        sql % (
            price_type_name,
            instrument_name,
            interval_name
        )
    )

    filename = output_directory + '/' + 'NEW_raw_pdf.parquet'
    pdf.to_parquet(filename)
    

    
with DAG(
        dag_id = 'NEW_prepare_forex_data',
        start_date = datetime(2024, 1, 1),
        schedule_interval = None,
        catchup = False,
) as dag:

    # config-ish
    price_type_name = 'mid'
    instrument_name = 'EUR/USD'
    interval_name = 'Minute'

    retries_pull_forex_data = 1
    retry_delay_minutes_pull_forex_data = 5

    output_directory = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components/output/TEMP'

    
    
    task_pull_forex_data = PythonOperator(
        task_id = 'task_pull_forex_data',
        python_callable = pull_forex_data,
        op_kwargs = {
            'price_type_name' : price_type_name,
            'instrument_name' : instrument_name,
            'interval_name' : interval_name,
            'output_directory' : output_directory,
        },
        retries = retries_pull_forex_data,
        retry_delay = timedelta(
            minutes = retry_delay_minutes_pull_forex_data,
        ),
    )

    task_pull_forex_data

    



