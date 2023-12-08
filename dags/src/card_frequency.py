import pickle 
import pandas as pd

def card_frequency(data):
    '''
        Calculates the  frequency of card usage
    '''
    df = pickle.loads(data)
    freq = df.groupby('cc_num').size()
    df['cc_freq'] = df['cc_num'].apply(lambda x : freq[x])
    
    pkl_df = pickle.dumps(df)
    return pkl_df