

# Apache Airflow libraries
from airflow import DAG
from airflow.operators.python import PythonOperator

# common system libraries
from datetime import datetime, timedelta

# local libraries
from utilities.config import config
from utilities.pull_forex_data import pull_forex_data

#
# Define our DAG
#
with DAG(
        dag_id = 'NEW_prepare_forex_data',    # change this at some point
        start_date = datetime(2024, 1, 1),
        schedule_interval = None,
        catchup = False,
) as dag:

    # Define the task that pulls candlestick data from the database
    task_pull_forex_data = PythonOperator(
        task_id = 'task_pull_forex_data',
        python_callable = pull_forex_data,
        op_kwargs = {
            'price_type_name' : config['price_type_name'],
            'instrument_name' : config['instrument_name'],
            'interval_name' : config['interval_name'],
            'output_file_name_and_path' : config['directory_output'] + '/' + config['filename_candlesticks_query_results'],
        },
        retries = config['retries_pull_forex_data'],
        retry_delay = timedelta(
            minutes = config['retry_delay_minutes_pull_forex_data'],
        ),
    )

    task_pull_forex_data



if __name__ == '__main__':
    dag.test()



