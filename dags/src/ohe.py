import pickle
import pandas as pd

def ohe(data):
    df_train_set = pickle.loads(data)

    categorical_cols = ['category', 'gender', 'state']

    df_train_set = pd.get_dummies(df_train_set, columns = categorical_cols)

    clustered_data = pickle.dumps(df_train_set)
    return clustered_data