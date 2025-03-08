

# Apache Airflow libraries
from airflow import DAG
from airflow.operators.python import PythonOperator

# common system libraries
from datetime import datetime, timedelta

# local libraries
from forex.prepare_forex_data.config import config
from forex.prepare_forex_data.pull_forex_data import pull_forex_data

#
# Define our DAG
#
with DAG(
        dag_id = config['dag_id'],   # this may not work in the UI
        start_date = datetime(2024, 1, 1),    # change this at some point
        schedule_interval = None,
        catchup = False,
) as dag:

    # Define the task that pulls candlestick data from the database
    task_pull_forex_data = PythonOperator(
        task_id = 'task_pull_forex_data',
        python_callable = pull_forex_data,
        op_kwargs = config,
        retries = config['retries_pull_forex_data'],
        retry_delay = timedelta(
            minutes = config['retry_delay_minutes_pull_forex_data'],
        ),
    )

    task_pull_forex_data



if __name__ == '__main__':
    dag.test()



