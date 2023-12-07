import pickle

def drop_columns(data):
    df = pickle.loads(data)
    df.drop(columns = ['Unnamed: 0', 'trans_num', 'unix_time', 'first', 'last', 'city', 'street', 'lat', 'long', 'zip'], inplace = True)
    pkl_df = pickle.dumps(df)
    return pkl_df
