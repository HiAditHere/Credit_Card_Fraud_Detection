import pickle

def merge_category(data):
    df_train_set = pickle.loads(data)

    cat = ['misc_net', 'grocery_pos', 'shopping_net', 'shopping_pos']
    def convert(x):
        if x not in cat:
            return('other')
        else:
            return x

    df_train_set.category = df_train_set.category.apply(lambda x: convert(x))

    pkl_df = pickle.dumps(df_train_set)
    return pkl_df