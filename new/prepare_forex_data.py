

# Apache Airflow libraries
from airflow import DAG
from airflow.operators.python import PythonOperator

# common system libraries
from datetime import datetime, timedelta

# local libraries
from forex.pre_training_data_prep.config import config



###########
#   DAG   #
###########

with DAG(
        dag_id = config['dag_id'],   # this may not work in the UI
        start_date = datetime(2024, 1, 1),    # change this at some point
        schedule_interval = None,
        catchup = False,
) as dag:

    ###############################################
    #   Pull candlestick data from the database   #
    ###############################################
    
    from forex.pre_training_data_prep.pull_forex_data import pull_forex_data

    task_pull_forex_data = PythonOperator(
        task_id = 'task_pull_forex_data',
        python_callable = pull_forex_data,
        op_kwargs = config,
        retries = config['retries_pull_forex_data'],
        retry_delay = timedelta(
            minutes = config['retry_delay_minutes_pull_forex_data'],
        ),
    )

    ################################
    #   Add timezone information   #
    ################################
        
    from forex.pre_training_data_prep.add_timezone_information import add_timezone_information

    task_add_timezone_information = PythonOperator(
        task_id = 'task_add_timezone_information',
        python_callable = add_timezone_information,
        op_kwargs = config,
        retries = config['retries_pull_forex_data'],
        retry_delay = timedelta(
            minutes = config['retry_delay_minutes_pull_forex_data'],
        ),
    )

    ###############################
    #   Assemble DAG from tasks   #
    ###############################
    
    #[ task_pull_forex_data ] >> task_add_timezone_information
    task_add_timezone_information

    
#####################################
#   Enable command-line execution   #
#####################################

if __name__ == '__main__':
    dag.test()



