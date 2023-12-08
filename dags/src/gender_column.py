import pickle
import pandas as pd

def gender_ohe(data):
    '''
        One Hot Encodes the 'gender' colunm
    '''
    df = pickle.loads(data)
    #Convert gender to binary classification
    df = pd.get_dummies(df,columns=['gender'],drop_first=True)
    
    pkl_df = pickle.dumps(df)
    return pkl_df