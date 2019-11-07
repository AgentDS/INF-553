#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/7/19 12:09 AM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : task2_experiment.py
# @Software: PyCharm
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext
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


def emit_idx_rating_test(lines, uid2idx_bc, bid2idx_bc):
    for line in lines:
        if line[0] in uid2idx_bc.value.keys():
            if line[1] in bid2idx_bc.value.keys():
                yield Rating(uid2idx_bc.value[line[0]], bid2idx_bc.value[line[1]], line[2])
            else:
                yield Rating(uid2idx_bc.value[line[0]], -1, line[2])
        else:
            if line[1] in bid2idx_bc.value.keys():
                yield Rating(-1, bid2idx_bc.value[line[1]], line[2])
            else:
                yield Rating(-1, -1, line[2])


def run_all(numPartitions, rank, numIter, lmbda):
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
                raw_val.append([row[0], row[1], float(row[2])])

    # [user_id, business_id, stars]
    sc = SparkContext.getOrCreate()
    train_data = sc.parallelize(raw_train, numPartitions)
    val_data = sc.parallelize(raw_val, numPartitions)

    user_ids = train_data.map(lambda x: x[0]).distinct().collect()
    user_cnt = len(user_ids)
    business_ids = train_data.map(lambda x: x[1]).distinct().collect()
    business_cnt = len(business_ids)

    uid2idx = dict()
    bid2idx = dict()
    idx2bid = dict()
    for idx, uid in enumerate(user_ids, 0):
        uid2idx[uid] = idx

    for idx, bid in enumerate(business_ids, 0):
        bid2idx[bid] = idx
        idx2bid[idx] = bid

    idx2bid_bc = sc.broadcast(idx2bid)
    uid2idx_bc = sc.broadcast(uid2idx)
    bid2idx_bc = sc.broadcast(bid2idx)

    train_ratings = train_data.mapPartitions(lambda lines: emit_idx_rating(lines, uid2idx_bc, bid2idx_bc))
    val_ratings = val_data.mapPartitions(lambda lines: emit_idx_rating_test(lines, uid2idx_bc, bid2idx_bc))

    model = ALS.train(train_ratings, rank, numIter, lmbda)

    # Evaluate the model on training data
    train_pairs = train_ratings.map(lambda p: (p[0], p[1]))
    train_predictions = model.predictAll(train_pairs).map(lambda r: ((r[0], r[1]), r[2]))
    train_ratesAndPreds = train_ratings.map(lambda r: ((r[0], r[1]), r[2])).join(train_predictions)
    train_RMSE = train_ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean() ** 0.5

    # Evaluate the model on validation data
    val_pairs = val_ratings.map(lambda p: (p[0], p[1]))
    val_predictions = model.predictAll(val_pairs).map(lambda r: ((r[0], r[1]), r[2]))
    val_ratesAndPreds = val_ratings.map(lambda r: ((r[0], r[1]), r[2])).join(val_predictions)
    val_RMSE = val_ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean() ** 0.5

    end = time()
    return train_RMSE, val_RMSE, int(end - start)


if __name__ == "__main__":
    ranks = [2, 3, 4, 5, 6, 7]
    lambdas = [0.01, 0.02, 0.04, 0.08, 0.1, 0.2, 0.4, 0.8]
    numIters = [10, 15, 20]
    partition_num = [2, 3, 4, 5, 7, 9]

    # TODO: rank 3 lambda 0.1, val_RMSE 1.07 ?????
    with open("./task2_experiment.txt", 'w') as log_file:
        print("rank         lambda         numIter         numPartitions         train_RMSE         val_RMSE         duration",
              file=log_file)
        print("rank         lambda         numIter         numPartitions         train_RMSE         val_RMSE         duration")
        for rank, lmbda, numIter, numPartitions in itertools.product(ranks, lambdas, numIters, partition_num):
            train_RMSE, val_RMSE, duration = run_all(numPartitions, rank, numIter, lmbda)
            print("%2d           %4.3f            %2d                %2d                 %6.5f           %6.5f           %3d" % (
                rank, lmbda, numIter, numPartitions, train_RMSE, val_RMSE, duration))
            print("%2d           %4.3f            %2d                %2d                 %6.5f           %6.5f           %3d" % (
                rank, lmbda, numIter, numPartitions, train_RMSE, val_RMSE, duration), file=log_file)
