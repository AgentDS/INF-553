#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/18/19 6:45 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : model_based_experiments.py
# @Software: PyCharm
import sys
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext, SparkConf
import csv
from time import time


def emit_missing_pair(lines, uid2idx_bc, bid2idx_bc):
    for line in lines:
        if line[0] not in uid2idx_bc.value.keys() or line[1] not in bid2idx_bc.value.keys():
            yield (line[0], line[1])


def emit_existing_pair(lines, uid2idx_bc, bid2idx_bc):
    for line in lines:
        if line[0] in uid2idx_bc.value.keys():
            if line[1] in bid2idx_bc.value.keys():
                yield (uid2idx_bc.value[line[0]], bid2idx_bc.value[line[1]])


def emit_idx_rating(lines, uid2idx_bc, bid2idx_bc):
    for line in lines:
        yield Rating(uid2idx_bc.value[line[0]], bid2idx_bc.value[line[1]], line[2])


def emit_avg_pairs(lines, avg_bc):
    for line in lines:
        yield [line[0], line[1], avg_bc.value]


def emit_id_rating_pairs(lines, uidx2id_bc, bidx2id_bc):
    for line in lines:
        yield [uidx2id_bc.value[line[0]], bidx2id_bc.value[line[1]], line[2]]


def cal_RMSE(output_file_path, test_file_path):
    conf = SparkConf().set('spark.driver.host', '127.0.0.1')
    sc = SparkContext(master='local', appName='myAppName', conf=conf)
    raws = []
    with open(output_file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raws.append([row[0] + ',' + row[1], float(row[2])])
    predict_val_pair = sc.parallelize(raws)

    raws = []
    with open(test_file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raws.append([row[0] + ',' + row[1], float(row[2])])
    ground_val_pair = sc.parallelize(raws)

    val_RMSE = (ground_val_pair.join(predict_val_pair).map(lambda x: (x[1][0] - x[1][1]) ** 2).mean()) ** 0.5
    sc.stop()
    return val_RMSE


def model_based_CF(train_file_path, test_file_path, output_file_path):
    raw_train = []
    with open(train_file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raw_train.append([row[0], row[1], float(row[2])])

    raw_test = []
    with open(test_file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raw_test.append([row[0], row[1]])

    # [user_id, business_id, stars]
    conf = SparkConf().set('spark.driver.host', '127.0.0.1')
    sc = SparkContext(master='local', appName='myAppName', conf=conf)
    train_data = sc.parallelize(raw_train, 5)
    test_data = sc.parallelize(raw_test, 5)

    avg_train = train_data.map(lambda x: x[2]).mean()
    avg_train_bc = sc.broadcast(avg_train)

    user_ids = train_data.map(lambda x: x[0]).distinct().collect()
    user_cnt = len(user_ids)
    business_ids = train_data.map(lambda x: x[1]).distinct().collect()
    business_cnt = len(business_ids)

    uid2idx = dict()
    bid2idx = dict()
    uidx2id = dict()
    bidx2id = dict()
    for idx, uid in enumerate(user_ids, 0):
        uid2idx[uid] = idx
        uidx2id[idx] = uid

    for idx, bid in enumerate(business_ids, 0):
        bid2idx[bid] = idx
        bidx2id[idx] = bid

    uidx2id_bc = sc.broadcast(uidx2id)
    bidx2id_bc = sc.broadcast(bidx2id)
    uid2idx_bc = sc.broadcast(uid2idx)
    bid2idx_bc = sc.broadcast(bid2idx)

    train_ratings = train_data.mapPartitions(lambda lines: emit_idx_rating(lines, uid2idx_bc, bid2idx_bc))
    model = ALS.train(train_ratings, 3, 15, 0.22)  # local optimal parameters

    # Evaluate the model on validation data
    exisitng_val_pairs = test_data.mapPartitions(lambda lines: emit_existing_pair(lines, uid2idx_bc, bid2idx_bc))
    missing_val_pairs = test_data.mapPartitions(lambda lines: emit_missing_pair(lines, uid2idx_bc, bid2idx_bc))

    # [uid,bid,rating]
    existing_val_predictions = model.predictAll(exisitng_val_pairs).mapPartitions(
        lambda lines: emit_id_rating_pairs(lines, uidx2id_bc, bidx2id_bc)).collect()
    missing_val_predictions = missing_val_pairs.mapPartitions(lambda lines: emit_avg_pairs(lines, avg_train_bc)).collect()
    sc.stop()

    with open(output_file_path, 'w') as out_f:
        print("user_id, business_id, prediction", file=out_f)
        for line in existing_val_predictions:
            print("{0},{1},{2:.10f}".format(line[0], line[1], line[2]), file=out_f)
        for line in missing_val_predictions:
            print("{0},{1},{2:.10f}".format(line[0], line[1], line[2]), file=out_f)


if __name__ == "__main__":
    prefix = "/Users/liangsiqi/Documents/Dataset/inf553final_competition/customized/"
    log_path = "./model_based_log.txt"
    rmse_hist = []
    duration_hist = []
    print("train-val        duration         RMSE")
    for i in range(1, 5):
        train_path = prefix + "subtrain%d.csv" % i
        test_path = prefix + "subval%d.csv" % i
        start = time()
        model_based_CF(train_path, test_path, prefix + "./pred%d.csv" % i)
        end = time()
        rmse = cal_RMSE(prefix + "./pred%d.csv" % i, test_path)
        duration = int(end - start)
        rmse_hist.append(rmse)
        duration_hist.append(duration)
        print("{0:d}                {1:3d}s             {2:.10f}".format(i, duration, rmse))
    train_path = "/Users/liangsiqi/Documents/Dataset/inf553final_competition/yelp_train.csv"
    test_path = "/Users/liangsiqi/Documents/Dataset/inf553final_competition/yelp_val.csv"
    start = time()
    model_based_CF(train_path, test_path, "/Users/liangsiqi/Documents/Dataset/inf553final_competition/" + "val_pred.csv")
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
