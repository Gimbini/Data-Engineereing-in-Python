# Pipeline : CSV -> JSON
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

def CSVtoJSON():
    df=pd.read_csv('data.CSV')
    for i,r in df.iterrows():
        print(r['name'])
    df.to_json('fromAirflow.JSON', orient='records')

# arguments for Airflow
default_args = {
    'owner': 'BinKim',
    'start_date': dt.datetime(2020,3,18),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

''' crontab
   *      *        *          *         *
minute, hour, day of month, month, day of week

@yearly - 0 0 1 1 * : 00:00 1st of Jan, any day of week.
'''

# Set up DAG for Airflow
with DAG('MyCSVDAG',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5), # or something like '0 * * * *'
         ) as dag:

    print_starting = BashOperator(task_id='starting',
                                  bash_command='echo "Reading CSV..."')
    CSVJson = PythonOperator(task_id='convertCSVtoJson',
                             python_callable=CSVtoJSON # calling the function created above
                             )

print_starting.set_downstream(CSVJson)

''' making connections between tasks
print_starting.set_downstream(CSVJson)
CSVJson.set_upstream(print_starting)
print_starting >> CSV Json          # called bit shift operator
CSVJson << print_starting

All 4 lines above mean the same.

However, write in consistent manner.
'''
