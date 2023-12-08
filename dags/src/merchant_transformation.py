import pickle
import pandas as pd

def merchant_transformations(data):
    '''
       Cleans the 'merchant' column
    '''
    df = pickle.loads(data)
    df['merchant'] = df['merchant'].apply(lambda x : x.replace('fraud_',''))
    
    pkl_df = pickle.dumps(df)
    return pkl_df