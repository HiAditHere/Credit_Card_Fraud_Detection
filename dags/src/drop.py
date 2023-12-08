import pickle
import pandas as pd

def drop_col(data):
    ''' 
        Drops the unwanted columns and reorders the columns of DataFrame
    '''
    
    df = pickle.loads(data)
    
    df.drop(columns=['cc_num','trans_date_trans_time','city_pop'],inplace=True)
    #Reorder columns
    df = df[['cc_freq','city','job','age','gender_M','merchant', 'category',
            'distance_km','month','day','hour','hours_diff_bet_trans','amt','is_fraud']]
    
    pkl_df = pickle.dumps(df)
    return pkl_df