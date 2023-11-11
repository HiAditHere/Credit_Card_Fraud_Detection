# Import necessary libraries and modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from data_preprocessing import load_data, data_preprocessing, drop, convert_strdate_to_datetime, slicer, merge_category, drop_2, ohe
from airflow import configuration as conf

conf.set('core', 'enable_xcom_pickling', 'True')

default_args = {
    'owner': 'team5',
    'start_date': datetime(2023, 11, 10),
    'retries': 0, # Number of retries in case of task failure
    'retry_delay': timedelta(minutes=5), # Delay before retries
}

dag = DAG(
    'pipeline',
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

'''data_preprocessing_task = PythonOperator(
    task_id='data_preprocessing_task',
    python_callable=data_preprocessing,
    op_args=[load_data_task.output],
    dag=dag,
)'''

drop_task = PythonOperator(
    task_id='drop_task',
    python_callable=drop,
    op_args=[load_data_task.output],
    dag=dag,
)

convert_strdate_to_datetime_task = PythonOperator(
    task_id='Convert_string_date_to_datetime',
    python_callable=convert_strdate_to_datetime,
    op_args=[drop_task.output],
    dag=dag,
)

slicer_task = PythonOperator(
    task_id='Slice_1_year_data',
    python_callable=slicer,
    op_args=[convert_strdate_to_datetime_task.output],
    dag=dag,
)

merge_category_task = PythonOperator(
    task_id='Merge_Columns',
    python_callable=merge_category,
    op_args=[slicer_task.output],
    dag=dag,
)

drop_2_task = PythonOperator(
    task_id='Drop_columns',
    python_callable=drop_2,
    op_args=[merge_category_task.output],
    dag=dag,
)

ohe_task = PythonOperator(
    task_id='OHE',
    python_callable=ohe,
    op_args=[drop_2_task.output],
    dag=dag,
)

#load_data_task >> data_preprocessing_task
load_data_task >> drop_task >> convert_strdate_to_datetime_task >> slicer_task >> merge_category_task >> drop_2_task >> ohe_task

# If this script is run directly, allow command-line interaction with the DAG
if __name__ == "__main__":
    dag.cli() 