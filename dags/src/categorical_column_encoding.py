import pickle
import pandas as pd
from category_encoders import WOEEncoder

def encode_categorical_col(data):
    ''' Encodes Categorical Col using WOE Encoder'''
    df = pickle.loads(data)
   
    for col in ['city','job','merchant', 'category']:
        df[col] = WOEEncoder().fit_transform(df[col],df['is_fraud'])
    
    pkl_df = pickle.dumps(df)
    return pkl_df    