import pickle
import pandas as pd
def extracting_age(data):
    '''
        Computes new column 'age' using the 'dob' column
        and drops the 'dob' column
    '''
    
    df = pickle.loads(data)

    df['dob'] = pd.to_datetime(df['dob'],format='mixed')
    df['age'] = (df['trans_date_trans_time'].dt.year - df['dob'].dt.year).astype(int)
    df.drop(columns='dob',inplace=True)
    
    pkl_df = pickle.dumps(df)
    return pkl_df
    
    