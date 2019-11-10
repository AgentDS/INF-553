#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/8/19 11:33 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : task2_user_base_ex.py
# @Software: PyCharm
import sys
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext, SparkConf
import csv
from time import time


def emit_rating_pairs_user_base(lines, uid2idx_bc, bid2idx_bc, key='user', method=1):
    if key == 'user':
        if method == 1:
            for line in lines:
                yield (uid2idx_bc.value[line[0]],
                       [(bid2idx_bc.value[line[1]], float(line[2]))])
        elif method == 2:
            for line in lines:
                yield (uid2idx_bc.value[line[0]], [bid2idx_bc.value[line[1]]])
    elif key == 'business':
        for line in lines:
            yield (bid2idx_bc.value[line[1]], [uid2idx_bc.value[line[0]]])


def rating_dict(rating_lines):
    for line in rating_lines:
        yield (line[0], dict(line[1]))


def emit_pairs_user_base(lines, uid2idx_bc, bid2idx_bc):
    for line in lines:
        pair = []
        if line[0] in uid2idx_bc.value.keys():
            pair.append(uid2idx_bc.value[line[0]])
        else:
            pair.append(line[0])
        if line[1] in bid2idx_bc.value.keys():
            pair.append(bid2idx_bc.value[line[1]])
        else:
            pair.append(line[1])
        yield pair


def emit_id_pairs_user_base(lines, uidx2id_bc, bidx2id_bc):
    for line in lines:
        if isinstance(line[0], int):
            uid = uidx2id_bc.value[line[0]]
        else:
            uid = line[0]
        if isinstance(line[1], int):
            bid = bidx2id_bc.value[line[1]]
        else:
            bid = line[1]
        string = '{},{},{:.10f}'.format(uid, bid, line[2])
        yield string


def cal_weight(user1_ratings, user2_ratings):
    # normalize first
    n = len(user1_ratings)
    mean1 = sum(user1_ratings) / float(n)
    mean2 = sum(user2_ratings) / float(n)
    user1_ratings = [i - mean1 for i in user1_ratings]
    user2_ratings = [i - mean2 for i in user2_ratings]
    numerator = sum([r1 * r2 for r1, r2 in zip(user1_ratings, user2_ratings)])
    denominator = (sum([r1 ** 2 for r1 in user1_ratings]) * sum([r2 ** 2 for r2 in user2_ratings])) ** 0.5
    if denominator > 0:
        pearson_sim = numerator / denominator
    else:
        pearson_sim = 0
    return pearson_sim, mean2


def emit_id_rating_pairs(lines, uidx2id_bc, bidx2id_bc):
    for line in lines:
        yield [uidx2id_bc.value[line[0]], bidx2id_bc.value[line[1]], line[2]]


def emit_avg_pairs(lines, avg_bc):
    for line in lines:
        yield [line[0], line[1], avg_bc.value]


def user_based_predict(pairs, user_ratings_bc, user_bidxs_bc, business_uidx_bc, global_avg_bc, neighbor_K):
    for pair in pairs:
        user1 = pair[0]
        business = pair[1]

        if isinstance(user1, int) and isinstance(business, int):
            weighted_ratings = []
            weights = []
            corated_users = business_uidx_bc.value[business]
            rated_bidx_set1 = set(user_bidxs_bc.value[user1])  # {bidx1, bidx2,...} for user1
            for user2 in corated_users:
                # corated business idx for user1 and user2: [bidx1, bidx2, ...]
                cobusiness = list(set(user_bidxs_bc.value[user2]).intersection(rated_bidx_set1))
                if len(cobusiness) > 1:
                    user1_rating = [user_ratings_bc.value[user1][i] for i in cobusiness]
                    user2_rating = [user_ratings_bc.value[user2][i] for i in cobusiness]
                    sim, mean2 = cal_weight(user1_rating, user2_rating)
                    weighted_ratings.append(sim * (user_ratings_bc.value[user2][business] - mean2))
                    weights.append(sim)
            # use top-K similar users' ratings to predict
            if neighbor_K < len(weights):
                sorted_weights_arg = sorted(range(len(weights)), key=weights.__getitem__)
                sorted_weights_arg.reverse()
                abs_weight_sum = sum([abs(weights[i]) for i in sorted_weights_arg[:neighbor_K]])
                weighted_ratings_sum = sum([weighted_ratings[i] for i in sorted_weights_arg[:neighbor_K]])
            else:
                abs_weight_sum = sum([abs(i) for i in weights])
                weighted_ratings_sum = sum(weighted_ratings)

            tmp = user_ratings_bc.value[user1].values()
            avg_rating1 = sum(tmp) / len(tmp)
            if abs_weight_sum > 0:
                pred = avg_rating1 + weighted_ratings_sum / abs_weight_sum
            else:
                pred = avg_rating1
        else:
            pred = global_avg_bc.value
        yield [user1, business, pred]


def user_based_CF(train_file_path, test_file_path, output_file_path, numPartitions):
    conf = SparkConf().set('spark.driver.host', '127.0.0.1')
    sc = SparkContext(master='local', appName='myAppName', conf=conf)

    raw_train = sc.textFile(train_file_path, numPartitions)
    header = raw_train.first()
    train_data = raw_train.filter(lambda x: x != header).mapPartitions(lambda x: csv.reader(x))

    global_avg = train_data.map(lambda x: float(x[2])).mean()
    global_avg_bc = sc.broadcast(global_avg)

    user_ids = train_data.map(lambda x: x[0]).distinct().collect()
    # user_cnt = len(user_ids)
    business_ids = train_data.map(lambda x: x[1]).distinct().collect()
    # business_cnt = len(business_ids)

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

    # (uidx, [(bidx1, rating1),
    #         (bidx2, rating2),
    #          ...])
    users_column = train_data.mapPartitions(
        lambda lines: emit_rating_pairs_user_base(lines, uid2idx_bc, bid2idx_bc, key='user')).reduceByKey(
        lambda a, b: a + b)
    users_column.persist()

    # {uidx: {bidx1: rating1, bidx2: rating2, ...},
    #  ...}
    user_ratings = users_column.mapPartitions(lambda cols: rating_dict(cols)).collectAsMap()
    user_ratings_bc = sc.broadcast(user_ratings)
    users_column.unpersist()

    user_bidxs = train_data.mapPartitions(
        lambda lines: emit_rating_pairs_user_base(lines, uid2idx_bc, bid2idx_bc, key='user', method=2)).reduceByKey(
        lambda a, b: a + b).collectAsMap()
    user_bidxs_bc = sc.broadcast(user_bidxs)

    # {bidx:[uidx1, uidx2, ...],
    #  ...}
    business_uidx = train_data.mapPartitions(
        lambda lines: emit_rating_pairs_user_base(lines, uid2idx_bc, bid2idx_bc, key='business')).reduceByKey(
        lambda a, b: a + b).collectAsMap()
    business_uidx_bc = sc.broadcast(business_uidx)

    raw_test = sc.textFile(test_file_path, numPartitions)
    test_header = raw_test.first()
    # [uid,bid,str(score)]
    test_data = raw_test.filter(lambda x: x != test_header).mapPartitions(lambda x: csv.reader(x))
    # [uidx,bidx]
    test_pairs = test_data.mapPartitions(lambda lines: emit_pairs_user_base(lines, uid2idx_bc, bid2idx_bc))
    test_pairs.persist()
    # ['uid,bid,pred']
    test_pred_id_pairs = test_pairs.mapPartitions(
        lambda pairs: user_based_predict(pairs, user_ratings_bc, user_bidxs_bc, business_uidx_bc, global_avg_bc,
                                         neighbor_K=20)).mapPartitions(
        lambda lines: emit_id_pairs_user_base(lines, uidx2id_bc, bidx2id_bc)).collect()
    sc.stop()
    with open(output_file_path, 'w') as out_f:
        print("user_id, business_id, prediction", file=out_f)
        for line in test_pred_id_pairs:
            print(line, file=out_f)


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


if __name__ == "__main__":
    train_file_path = "/Users/liangsiqi/Documents/Dataset/yelp_rec_data/yelp_train.csv"
    test_file_path = "/Users/liangsiqi/Documents/Dataset/yelp_rec_data/yelp_val.csv"
    output_file_path = "./task2_local_user_based.csv"
    log_path = "./task2_local_user_based_log.txt"
    duration_hist = []
    RMSE_hist = []
    partition_range = 3
    candidate_K = [15, 17, 20, 23, 25]
    for neighbor_k in candidate_K:
        start = time()
        user_based_CF(train_file_path, test_file_path, output_file_path, 3, 20)
        duration = int(time() - start)
        test_RMSE = cal_RMSE()
        duration_hist.append(duration)
        RMSE_hist.append(test_RMSE)
        print("neighbor_K = %d, Dutation = %d, RMSE = %.6f" % (neighbor_k, duration, test_RMSE))
    with open(log_path, 'w') as log_f:
        for i in range(len(candidate_K)):
            print("neighbor_K = %d, Dutation = %d, RMSE = %.6f" % (candidate_K[i], duration_hist[i], RMSE_hist[i]), file=log_f)
