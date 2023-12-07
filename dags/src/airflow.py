import pandas as pd
import pickle
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import configuration as conf

from src.drop_columns import drop_columns
from src.merge_category import merge_category
from src.ohe import ohe
from src.woe_merchant import woe_merchant

conf.set('core', 'enable_xcom_pickling', 'True')
conf.set('core', 'enable_parquet_xcom', 'True')

def load_data():

    df = pd.DataFrame()
    chunk_size = 10000  # Adjust the chunk size as needed

    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    chunks = pd.read_csv(os.path.join(root_dir, "data/file.csv"), chunksize=chunk_size)
    for chunk in chunks:
        df = pd.concat([df, chunk], axis = 0)
        break
    
    df = pd.read_csv(os.path.join(root_dir, "data/file.csv"))

    serialized_data = pickle.dumps(df)
    return serialized_data

def write_data(data):
    df = pickle.loads(data)
    
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    df.to_csv(os.path.join(root_dir, "data/file.csv"),index=False)

default_args = {
    'owner': 'team5',
    'start_date': datetime(2023, 11, 11),
    'retries': 0, # Number of retries in case of task failure
    'retry_delay': timedelta(minutes=5), # Delay before retries
}

dag = DAG(
    'pipeline2',
    default_args=default_args,
    description='Pipeline',
    schedule_interval=None, # Set the schedule interval or use None for manual triggering
    catchup=False,
)

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    executor_config={"memory": "8192Mi"},
    dag=dag,
)

drop_task = PythonOperator(
    task_id='drop_columns',
    python_callable=drop_columns,
    op_args=[load_data_task.output],
    executor_config={"memory": "8192Mi"},
    dag=dag,
)

merge_category_task = PythonOperator(
    task_id='Merge_Columns',
    python_callable=merge_category,
    op_args=[drop_task.output],
    executor_config={"memory": "8192Mi"},
    dag=dag,
)

woe_task = PythonOperator(
    task_id='WOE',
    python_callable=woe_merchant,
    op_args=[merge_category_task.output],
    executor_config={"memory": "8192Mi"},
    dag=dag,
)

ohe_task = PythonOperator(
    task_id='OHE',
    python_callable=ohe,
    op_args=[woe_task.output],
    executor_config={"memory": "8192Mi"},
    dag=dag,
)

write_to_file = PythonOperator(
    task_id = 'write_to_file',
    python_callable = write_data,
    op_args=[ohe_task.output],
    executor_config={"memory": "8192Mi"},
    dag=dag,
)

load_data_task >> drop_task >> merge_category_task >> woe_task >> ohe_task >> write_to_file

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.cli()