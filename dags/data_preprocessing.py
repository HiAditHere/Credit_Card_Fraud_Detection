import pandas as pd
#from kneed import KneeLocator
import pickle
import os
from datetime import datetime

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
