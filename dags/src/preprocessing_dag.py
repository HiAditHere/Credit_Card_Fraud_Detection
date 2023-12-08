
import pickle
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import configuration as conf

from src.date_transformations import date_transformations
from src.merchant_transformation import merchant_transformations
from src.extracting_age import extracting_age
from src.distance_calculation import distance_calculation
from src.gender_column import gender_ohe
from src.transaction_gap import transaction_gap
from src.card_frequency import card_frequency
from src.drop import drop_col
from src.categorical_column_encoding import encode_categorical_col


def write_data(data):
    df = pickle.loads(data)
    
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    df.to_csv(os.path.join(root_dir, "data/file.csv"),index=False)

conf.set('core', 'enable_xcom_pickling', 'True')
conf.set('core', 'enable_parquet_xcom', 'True')

def load_data():
    import pandas as pd
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



default_args = {
    'owner': 'team5',
    'start_date': datetime(2023, 11, 11),
    'retries': 0, # Number of retries in case of task failure
    'retry_delay': timedelta(minutes=5), # Delay before retries
}

dag = DAG(
    'preprocessing_pipeline',
    default_args=default_args,
    description='preprocessing pipeline',
    schedule_interval=None, 
    catchup=False,
)

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag,
)


date_column_task = PythonOperator(
    task_id = 'date_column_task',
    python_callable = date_transformations,
    op_args = [load_data_task.output],
    dag=dag,
)


merchant_column_task = PythonOperator(
    task_id = 'merchant_column_task',
    python_callable = date_transformations,
    op_args = [date_column_task.output],
    dag=dag,
)

dob_column_task = PythonOperator(
    task_id = 'dob_column_task',
    python_callable = extracting_age,
    op_args = [merchant_column_task.output],
    dag=dag,
)

distance_task = PythonOperator(
    task_id = 'distance_task',
    python_callable = distance_calculation,
    op_args = [dob_column_task.output],
    dag =dag,
)

ohe_task = PythonOperator(
    task_id = 'ohe_task',
    python_callable = gender_ohe,
    op_args = [distance_task.output],
    dag = dag,  
)

transaction_gap_task = PythonOperator(
    task_id = 'transaction_gap',
    python_callable = transaction_gap,
    op_args = [ohe_task.output],
    dag = dag, 
)

card_frequency_task = PythonOperator(
    task_id = 'ard_frequency_task',
    python_callable = card_frequency,
    op_args = [transaction_gap_task.output],
    dag = dag, 
)

drop_task = PythonOperator(
    task_id = 'drop_task',
    python_callable = drop_col,
    op_args = [card_frequency_task.output],
    dag = dag,
)

categorical_columns_task = PythonOperator(
    task_id = 'categorical_columns_task',
    python_callable = encode_categorical_col,
    op_args = [drop_task.output],
    dag = dag, 
)

write_to_file = PythonOperator(
    task_id = 'write_to_file',
    python_callable = write_data,
    op_args=[categorical_columns_task.output],
    dag=dag,
)


load_data_task >> date_column_task >> merchant_column_task >> dob_column_task >> distance_task >> ohe_task >> transaction_gap_task >> card_frequency_task >> drop_task >> categorical_columns_task >> write_to_file

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.cli()