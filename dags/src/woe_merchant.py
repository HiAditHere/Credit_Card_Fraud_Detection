import pickle

def strip_fraud(x):
    return x.split(sep='_')[1]

def calculate_woe_iv(data, target_column, category_column):
    total_good = data[data[target_column] == 0].shape[0]
    total_bad = data[data[target_column] == 1].shape[0]

    woe_dict = {}

    for category in data[category_column].unique():
        good = data[(data[category_column] == category) & (data[target_column] == 0)].shape[0]
        bad = data[(data[category_column] == category) & (data[target_column] == 1)].shape[0]

        if good == 0:
            good_percentage = 0.5  # Avoid division by zero
        else:
            good_percentage = good / total_good

        if bad == 0:
            bad_percentage = 0.5  # Avoid division by zero
        else:
            bad_percentage = bad / total_bad

        if good_percentage == 0:
            woe = -float('inf')
        elif bad_percentage == 0:
            woe = float('inf')
        else:
            woe = round((bad / total_bad) / (good / total_good), 4)

        woe_dict[category] = woe

    return woe_dict

def woe_merchant(data):
    df = pickle.loads(data)

    df['merchant'] = df['merchant'].apply(lambda x: strip_fraud(x))
    woe_dict = calculate_woe_iv(df, 'is_fraud', 'merchant')
    df['merchant'] = df['merchant'].map(woe_dict)

    pkl_df = pickle.dumps(df)

    return pkl_df
