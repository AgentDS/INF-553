#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/7/19 12:09 AM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : task2_experiment.py
# @Software: PyCharm
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext, SparkConf
import csv
from time import time
import itertools
from itertools import combinations
from collections import OrderedDict, Counter
import random
from numpy.random import rand


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


def run_all(numPartitions, rank, numIter, lmbda, output_file):
    start = time()
    train_file = "/Users/liangsiqi/Documents/Dataset/yelp_rec_data/yelp_train.csv"
    val_file = "/Users/liangsiqi/Documents/Dataset/yelp_rec_data/yelp_val.csv"

    raw_train = []
    with open(train_file, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raw_train.append([row[0], row[1], float(row[2])])

    raw_val = []
    with open(val_file, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raw_val.append([row[0], row[1]])

    # [user_id, business_id, stars]
    conf = SparkConf().set('spark.driver.host', '127.0.0.1')
    sc = SparkContext(master='local', appName='myAppName', conf=conf)
    train_data = sc.parallelize(raw_train, numPartitions)
    val_data = sc.parallelize(raw_val, numPartitions)

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
    model = ALS.train(train_ratings, rank, numIter, lmbda)

    # Evaluate the model on training data
    train_pairs = train_ratings.map(lambda p: (p[0], p[1]))
    train_predictions = model.predictAll(train_pairs).map(lambda r: ((r[0], r[1]), r[2]))
    train_ratesAndPreds = train_ratings.map(lambda r: ((r[0], r[1]), r[2])).join(train_predictions)
    # train_RMSE = train_ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean() ** 0.5

    # Evaluate the model on validation data
    exisitng_val_pairs = val_data.mapPartitions(lambda lines: emit_existing_pair(lines, uid2idx_bc, bid2idx_bc))
    missing_val_pairs = val_data.mapPartitions(lambda lines: emit_missing_pair(lines, uid2idx_bc, bid2idx_bc))

    # [uid,bid,rating]
    existing_val_predictions = model.predictAll(exisitng_val_pairs).mapPartitions(
        lambda lines: emit_id_rating_pairs(lines, uidx2id_bc, bidx2id_bc)).collect()
    missing_val_predictions = missing_val_pairs.mapPartitions(lambda lines: emit_avg_pairs(lines, avg_train_bc)).collect()
    sc.stop()

    with open(output_file, 'w') as out_f:
        print("user_id, business_id, prediction", file=out_f)
        for line in existing_val_predictions:
            print(line[0] + ',' + line[1] + ',' + "%.10f" % line[2], file=out_f)
        for line in missing_val_predictions:
            print(line[0] + ',' + line[1] + ',' + "%.10f" % line[2], file=out_f)
    end = time()
    return int(end - start)


if __name__ == "__main__":
    ranks = [2, 3, 4, 5]
    lambdas = [0.08, 0.1, 0.2]
    numIters = [10, 15, 20]
    partition_num = [3, 4, 5]
    output_file = "./task2_local_res.csv"

    # TODO: rank 3 lambda 0.1, val_RMSE 1.07 ?????
    duration = run_all(numPartitions=5, rank=3, numIter=15, lmbda=0.2, output_file=output_file)
    conf = SparkConf().set('spark.driver.host', '127.0.0.1')
    sc = SparkContext(master='local', appName='myAppName', conf=conf)

    raws = []
    with open(output_file, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raws.append([row[0] + ',' + row[1], float(row[2])])
    predict_val_pair = sc.parallelize(raws)

    raws = []
    val_file = "/Users/liangsiqi/Documents/Dataset/yelp_rec_data/yelp_val.csv"
    with open(val_file, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raws.append([row[0] + ',' + row[1], float(row[2])])
    ground_val_pair = sc.parallelize(raws)

    val_RMSE = (ground_val_pair.join(predict_val_pair).map(lambda x: (x[1][0] - x[1][1]) ** 2).mean()) ** 0.5
    print("val_RMSE = %.5f, duration = %ds" % (val_RMSE, duration))
    sc.stop()

# with open("./task2_experiment.txt", 'w') as log_file:
# print("rank         lambda         numIter         numPartitions         train_RMSE         val_RMSE         duration", file=log_file)
# print("rank         lambda         numIter         numPartitions         train_RMSE         val_RMSE         duration")
# for rank, lmbda, numIter, numPartitions in itertools.product(ranks, lambdas, numIters, partition_num):
#     duration = run_all(numPartitions, rank, numIter, lmbda, output_file)

# print("%2d           %4.3f            %2d                %2d                 %6.5f           %6.5f           %3d" % (
#     rank, lmbda, numIter, numPartitions, train_RMSE, val_RMSE, duration))
# print("%2d           %4.3f            %2d                %2d                 %6.5f           %6.5f           %3d" % (
#     rank, lmbda, numIter, numPartitions, train_RMSE, val_RMSE, duration), file=log_file)
