import pickle
import pandas as pd
def date_transformations(data):
    '''
        Converts date column to datetime format and 
        extracts 3 new columns 'day' ,'hour', 'month'
    '''
    
    df = pickle.loads(data)
    df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'],format='mixed')
    df['hour'] = df['trans_date_trans_time'].dt.hour
    df['day'] = df['trans_date_trans_time'].dt.weekday
    df['month'] = df['trans_date_trans_time'].dt.month
    
    pkl_df = pickle.dumps(df)
    return pkl_df