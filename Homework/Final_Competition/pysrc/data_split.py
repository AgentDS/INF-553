import pandas as pd
import numpy as np
import csv
from sklearn.model_selection import StratifiedKFold

val_path = "../data/yelp_val.csv"
train_path = "../data/yelp_train.csv"

val_raw = pd.read_csv(val_path)
train_raw = pd.read_csv(train_path)

val_raw.columns = ['user_id','business_id','stars']
train_raw.columns = ['user_id','business_id','stars']

val_raw['stars'] = val_raw['stars'].astype(int)
train_raw['stars'] = train_raw['stars'].astype(int)

print("size of train: ",len(train_raw))
print("size of val: ",len(val_raw))

train_val = pd.concat([train_raw,val_raw],axis=0)
print("size of train + val: ",len(train_val))

print("Total number of distinct user id: ",len(train_val['user_id'].unique()))
print("Total number of distinct business id: ",len(train_val['business_id'].unique()))

skf = StratifiedKFold(n_splits=4, random_state=123, shuffle=False)
skf.get_n_splits(train_val, train_val['user_id'])
print("Split snitialize finished.")

i = 1
for train_index, test_index in skf.split(train_val, train_val['user_id']):
    X_train, X_test = train_val.iloc[train_index], train_val.iloc[test_index]
    X_train.to_csv("./subtrain%d.csv" % i,index_label=False,index=False)
    X_test.to_csv("./subval%d.csv" % i,index_label=False,index=False)
    i += 1
    print("%dth split finished" % (i-1))
