import pickle 
import pandas as pd
import numpy as np

def transaction_gap(data):
    '''
        Calculates the time difference between each card usage in hours
        If the card is being used for the firt time, the difference is set to 0 for that particular instance
    '''
    df = pickle.loads(data)
    
    df.sort_values(['cc_num', 'trans_date_trans_time'],inplace=True)
    df['hours_diff_bet_trans']=((df.groupby('cc_num')[['trans_date_trans_time']].diff())/np.timedelta64(1,'h'))
    df['hours_diff_bet_trans'].fillna(0,inplace=True)
    
    pkl_df = pickle.dumps(df)
    return pkl_df