#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/25/19 1:25 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task2.py
# @Software: PyCharm

import sys
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext, SparkConf
import csv
from time import time


def emit_idx_rating(lines, uid2idx_bc, bid2idx_bc):
    for line in lines:
        yield Rating(uid2idx_bc.value[line[0]], bid2idx_bc.value[line[1]], line[2])


def emit_existing_pair(lines, uid2idx_bc, bid2idx_bc):
    for line in lines:
        if line[0] in uid2idx_bc.value.keys():
            if line[1] in bid2idx_bc.value.keys():
                yield (uid2idx_bc.value[line[0]], bid2idx_bc.value[line[1]])


def emit_missing_pair(lines, uid2idx_bc, bid2idx_bc):
    for line in lines:
        if line[0] not in uid2idx_bc.value.keys() or line[1] not in bid2idx_bc.value.keys():
            yield (line[0], line[1])


def emit_id_rating_pairs(lines, uidx2id_bc, bidx2id_bc):
    for line in lines:
        yield [uidx2id_bc.value[line[0]], bidx2id_bc.value[line[1]], line[2]]


def emit_avg_pairs(lines, avg_bc):
    for line in lines:
        yield [line[0], line[1], avg_bc.value]


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
    model = ALS.train(train_ratings, 3, 15, 0.2)  # local optimal parameters

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
            print(line[0] + ',' + line[1] + ',' + "%.10f" % line[2], file=out_f)
        for line in missing_val_predictions:
            print(line[0] + ',' + line[1] + ',' + "%.10f" % line[2], file=out_f)


def user_based_CF(train_file_path, test_file_path, output_file_path):
    pass


if __name__ == "__main__":
    argv = sys.argv
    train_file_path = argv[1]
    test_file_path = argv[2]
    case_id = int(argv[3])
    output_file_path = argv[4]

    if case_id == 1:
        # Model-based CF
        # local test CPU time: 17.60s
        # local test yelp_val.csv RMSE: 1.06646
        model_based_CF(train_file_path, test_file_path, output_file_path)
    elif case_id == 2:
        # User-based CF
        pass
    elif case_id == 3:
        # Item-based CF
        pass
    else:
        pass
