import pickle
import pandas as pd
from geopy.distance import great_circle
   
def distance_calculation(data):
    '''
       Calculates distance between customer location
       and merchant location in km to a new column 'distance_km'.
       Drops 'lat','long','merch_lat','merch_long' columns
    '''
       
    df = pickle.loads(data)
    df['distance_km'] = df.apply(lambda col : round(great_circle((col['lat'],col['long']),
                                                (col['merch_lat'],col['merch_long'])).kilometers,2),axis=1)
    df.drop(columns=['lat','long','merch_lat','merch_long'],inplace=True)
    
    pkl_df = pickle.dumps(df)
    return pkl_df
    