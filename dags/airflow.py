# Import necessary libraries and modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
#from data_preprocessing import load_data, data_preprocessing, drop, convert_strdate_to_datetime, slicer, merge_category, drop_2, ohe
from airflow import configuration as conf
#from kneed import KneeLocator
import pickle
import pandas as pd
import os

conf.set('core', 'enable_xcom_pickling', 'True')

def load_data():

    df = pd.DataFrame()
    chunk_size = 10000  # Adjust the chunk size as needed
    chunks = pd.read_csv(os.path.join(os.path.dirname(__file__), "../data/file.csv"), chunksize=chunk_size)
    for chunk in chunks:
        df = pd.concat([df, chunk], axis = 0)
        break

    #df = pd.read_csv(os.path.join(os.path.dirname(__file__), "../data/file1.csv"))
    serialized_data = pickle.dumps(df)
    return serialized_data

def data_preprocessing(data):

    df = pickle.loads(data)

    df.drop(columns = ['Unnamed: 0', 'trans_num', 'unix_time', 'first', 'last', 'city', 'street'], inplace = True)  

    def convert(x):
        return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

    df.trans_date_trans_time = df.trans_date_trans_time.apply(lambda x: convert(x))

    df_train_set = df[df.trans_date_trans_time < datetime(2020, 1, 1)]

    cat = ['misc_net', 'grocery_pos', 'shopping_net', 'shopping_pos']
    def convert(x):
        if x not in cat:
            return('other')
        else:
            return x

    df_train_set.category = df_train_set.category.apply(lambda x: convert(x))

    df_train_set.drop(columns = ['lat', 'long', 'zip'], inplace = True)

    categorical_cols = ['category', 'gender', 'state']

    df_train_set = pd.get_dummies(df_train_set, columns = categorical_cols)

    clustered_data = pickle.dumps(df_train_set)
    return clustered_data

def drop(data):
    df = pickle.loads(data)
    df.drop(columns = ['Unnamed: 0', 'trans_num', 'unix_time', 'first', 'last', 'city', 'street'], inplace = True) 
    pkl_df = pickle.dumps(df)
    return pkl_df

def convert_strdate_to_datetime(data):
    df = pickle.loads(data)
    df.trans_date_trans_time = df.trans_date_trans_time.apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    pkl_df = pickle.dumps(df)
    return pkl_df

def slicer(data):
    df = pickle.loads(data)
    df_train_set = df[df.trans_date_trans_time < datetime(2020, 1, 1)]
    pkl_df = pickle.dumps(df_train_set)
    return pkl_df

def merge_category(data):
    df_train_set = pickle.loads(data)

    cat = ['misc_net', 'grocery_pos', 'shopping_net', 'shopping_pos']
    def convert(x):
        if x not in cat:
            return('other')
        else:
            return x

    df_train_set.category = df_train_set.category.apply(lambda x: convert(x))

    pkl_df = pickle.dumps(df_train_set)
    return pkl_df

def drop_2(data):
    df_train_set = pickle.loads(data)
    df_train_set.drop(columns = ['lat', 'long', 'zip'], inplace = True) 
    pkl_df = pickle.dumps(df_train_set)
    return pkl_df

def ohe(data):
    df_train_set = pickle.loads(data)

    categorical_cols = ['category', 'gender', 'state']

    df_train_set = pd.get_dummies(df_train_set, columns = categorical_cols)

    clustered_data = pickle.dumps(df_train_set)
    return clustered_data


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