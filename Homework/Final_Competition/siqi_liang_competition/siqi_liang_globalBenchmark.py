#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/2/19 12:57 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_globalBenchmark.py
# @Software: PyCharm
from pyspark import SparkContext
import pandas as pd
import csv
import json
from siqi_liang_utils import cal_RMSE, error_distribution, write_predict
from time import time
import ast
import re


def get_predict(pairs, global_user_avg, global_business_avg, user_profile, business_profile):
    for pair in pairs:
        uid = pair[0]
        bid = pair[1]
        user_avg = user_profile.get(uid, global_user_avg)
        business_avg = business_profile.get(bid, global_business_avg)
        pred = (user_avg + business_avg) / 2
        line = "{},{},{:.10f}".format(uid, bid, pred)
        yield line


def global_Benchmark(train_file_path, test_file_path, output_file_path, user_feature_path, business_feature_path, numPartitions):
    sc = SparkContext.getOrCreate()

    raw_user_profile = sc.textFile(user_feature_path, numPartitions)
    header = raw_user_profile.first()
    user_profile = raw_user_profile.filter(lambda x: x != header).mapPartitions(lambda x: csv.reader(x)).map(
        lambda x: (x[0], float(x[7]))).collectAsMap()
    user_tmp = list(user_profile.values())
    global_user_avg = sum(user_tmp) / len(user_tmp)

    raw_business_profile = sc.textFile(business_feature_path, numPartitions)
    header = raw_business_profile.first()
    business_profile = raw_business_profile.filter(lambda x: x != header).mapPartitions(lambda x: csv.reader(x)).map(
        lambda x: (x[0], float(x[2]))).collectAsMap()
    business_tmp = list(business_profile.values())
    global_business_avg = sum(business_tmp) / len(business_tmp)
    raw_test = sc.textFile(test_file_path, numPartitions)
    test_header = raw_test.first()
    # [uid,bid]
    test_pairs = raw_test.filter(lambda x: x != test_header).mapPartitions(lambda x: csv.reader(x)).map(lambda x: (x[0], x[1]))
    test_pairs.persist()
    test_pred_id_pairs = test_pairs.mapPartitions(
        lambda pairs: get_predict(pairs, global_user_avg, global_business_avg, user_profile, business_profile)).collect()
    sc.stop()
    write_predict(test_pred_id_pairs, output_file_path)


def user_extract(x):
    user_id = x['user_id']
    review_cnt = x['review_count']
    if x['friends'] == 'None':
        friends = 0
    else:
        friends = len(x['friends'])

    useful = x['useful']
    cool = x['cool']
    funny = x['funny']
    fans = x['fans']
    avg_star = x['average_stars']
    # user_id, review_cnt, friends, useful, cool, funny, fans, avg_star
    return (user_id, review_cnt, friends, useful, cool, funny, fans, avg_star)


def get_duration(string):
    tmp = re.split('[\-:]+', string)
    duration = float(int(tmp[2]) - int(tmp[0]))
    if duration < 0:
        duration += 24
    return duration


def business_extract(x):
    bid = x['business_id']
    state = x['state']
    latitude = x['latitude']
    longitude = x['longitude']
    star = x['stars']
    review_cnt = x['review_count']
    if x['categories'] is None:
        categories = []
    else:
        categories = sorted(x['categories'].split(', '))

    if x['hours'] is None:
        duration = -1
        mon = -1
        tue = -1
        wed = -1
        thu = -1
        fri = -1
        sat = -1
        sun = -1
    else:
        duration = len(x['hours'])
        hours = x['hours']
        if 'Monday' in hours:
            mon = get_duration(hours['Monday'])
        else:
            mon = 0
        if 'Tuesday' in hours:
            tue = get_duration(hours['Tuesday'])
        else:
            tue = 0
        if 'Wednesday' in hours:
            wed = get_duration(hours['Wednesday'])
        else:
            wed = 0
        if 'Thursday' in hours:
            thu = get_duration(hours['Thursday'])
        else:
            thu = 0
        if 'Friday' in hours:
            fri = get_duration(hours['Friday'])
        else:
            fri = 0
        if 'Saturday' in hours:
            sat = get_duration(hours['Saturday'])
        else:
            sat = 0
        if 'Sunday' in hours:
            sun = get_duration(hours['Sunday'])
        else:
            sun = 0

    BikeParking = 0
    NoiseLevel = -1
    GoodForKids = -1
    BusinessParking = -1

    RestaurantsPriceRange = -1
    if isinstance(x['attributes'], dict):
        attribute = x['attributes']
        if 'BikeParking' in attribute:
            if attribute['BikeParking'] == 'True':
                BikeParking = 1
        if 'NoiseLevel' in attribute:
            NoiseLevel = attribute['NoiseLevel']
        if 'RestaurantsPriceRange2' in attribute:
            RestaurantsPriceRange = int(attribute['RestaurantsPriceRange2'])
        if 'GoodForKids' in attribute:
            if attribute['GoodForKids'] == 'True':
                GoodForKids = 1
            if attribute['GoodForKids'] == 'False':
                GoodForKids = 0
        if 'BusinessParking' in attribute:
            acceptable_string = attribute['BusinessParking'].replace("'", "\"")
            BusinessParking_dict = ast.literal_eval(acceptable_string)
            if any(list(BusinessParking_dict.values())):
                BusinessParking = 1
            else:
                BusinessParking = 0

    # bid, state, star, latitude, longitude, review_cnt, BikeParking, BusinessParking, NoiseLevel, GoodForKids, categories, duration, mon, tue, wed, thu, fri, sat, sun
    return (
        bid, state, star, latitude, longitude, review_cnt, BikeParking, BusinessParking, NoiseLevel, GoodForKids, categories, duration, mon,
        tue, wed, thu, fri, sat, sun)


def make_feature_file(input_path, user_feature_file, business_feature_file):
    user_file = input_path + "user.json"
    business_file = input_path + "business.json"
    sc = SparkContext.getOrCreate()
    userRDD = sc.textFile(user_file).map(json.loads)
    user_profile = userRDD.map(user_extract)
    user_profile_data = user_profile.collect()
    user_profile_df = pd.DataFrame(data=user_profile.collect(),
                                   columns=['user_id', 'review_cnt',
                                            'friends', 'useful', 'cool',
                                            'funny', 'fans', 'avg_star'])

    user_profile_df.to_csv(user_feature_file, index=False)

    businessRDD = sc.textFile(business_file).map(json.loads)
    business_profile = businessRDD.map(lambda x: business_extract(x))
    business_profile_data = business_profile.collect()
    business_profile_df = pd.DataFrame(data=business_profile_data,
                                       columns=['business_id', 'state', 'star',
                                                'latitude', 'longitude', 'review_cnt',
                                                'BikeParking', 'BusinessParking',
                                                'NoiseLevel', 'GoodForKids', 'categories',
                                                'duration', 'mon', 'tue', 'wed', 'thu',
                                                'fri', 'sat', 'sun'])
    business_profile_df.to_csv(business_feature_file, index=False)


if __name__ == "__main__":
    prefix = "/Users/liangsiqi/Documents/Dataset/inf553final_competition/customized/"
    log_path = "./global_benchmark_log.txt"
    rmse_hist = []
    duration_hist = []
    print("train-val        duration         RMSE")
    for i in range(1, 5):
        train_path = prefix + "subtrain%d.csv" % i
        test_path = prefix + "subval%d.csv" % i
        start = time()
        global_Benchmark(None, test_path, prefix + "/pred%d.csv" % i, "./user_features.csv", "./business_features.csv", 4)
        end = time()
        rmse = cal_RMSE(prefix + "/pred%d.csv" % i, test_path)
        duration = int(end - start)
        rmse_hist.append(rmse)
        duration_hist.append(duration)
        print("{0:d}                {1:3d}s             {2:.10f}".format(i, duration, rmse))
    train_path = "/Users/liangsiqi/Documents/Dataset/inf553final_competition/yelp_train.csv"
    test_path = "/Users/liangsiqi/Documents/Dataset/inf553final_competition/yelp_val.csv"
    start = time()
    global_Benchmark(None, test_path, "/Users/liangsiqi/Documents/Dataset/inf553final_competition/" + "val_pred.csv",
                     "./user_features.csv", "./business_features.csv", 4)
    end = time()
    rmse = cal_RMSE("/Users/liangsiqi/Documents/Dataset/inf553final_competition/" + "val_pred.csv", test_path)
    duration = int(end - start)
    rmse_hist.append(rmse)
    duration_hist.append(duration)
    print("{0}                {1:3d}s             {2:.10f}".format('val', duration, rmse))
    with open(log_path, 'w') as log_f:
        print("train-val        duration         RMSE", file=log_f)
        for i in range(4):
            print("{0:d}                {1:3d}s             {2:.10f}".format(i, duration_hist[i], rmse_hist[i]), file=log_f)
        print("{0:d}                {1:3d}s             {2:.10f}".format(4, duration_hist[4], rmse_hist[4]), file=log_f)
