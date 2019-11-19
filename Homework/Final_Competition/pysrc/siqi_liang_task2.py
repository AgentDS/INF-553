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
import statistics
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
        if method == 1:
            for line in lines:
                yield (bid2idx_bc.value[line[1]], [float(line[2])])
        elif method == 2:
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


def cal_weight(user1_ratings, user2_ratings, corated_avg=True, user2_global_ratings=None):
    # normalize first
    n = len(user1_ratings)
    mean1 = sum(user1_ratings) / float(n)
    if corated_avg is True:
        mean2 = sum(user2_ratings) / float(n)
    else:
        mean2 = sum(user2_global_ratings) / len(user2_global_ratings)
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


def user_based_predict(pairs, user_ratings_bc, business_ratings_bc, user_bidxs_bc, business_uidx_bc, global_avg_bc, positive_sim=False,
                       neighbor_K=200000):
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
                    # sim, mean2 = cal_weight(user1_rating, user2_rating, corated_avg=True)
                    user2_global_ratings = list(user_ratings_bc.value[user2].values())
                    sim, mean2 = cal_weight(user1_rating, user2_rating, corated_avg=False, user2_global_ratings=user2_global_ratings)
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

            # only select users with positive similarity
            if positive_sim is True:
                tmp_cnt = len(weights)
                pos_weight_candidate = []
                weighted_ratings_candidate = []
                for i in range(tmp_cnt):
                    if weights[i] > 0:
                        pos_weight_candidate.append(weights[i])
                        weighted_ratings_candidate.append(weighted_ratings[i])
                abs_weight_sum = sum(pos_weight_candidate)
                weighted_ratings_sum = sum(weighted_ratings_candidate)

            tmp = user_ratings_bc.value[user1].values()
            avg_rating1 = sum(tmp) / len(tmp)
            if abs_weight_sum > 0:
                pred = avg_rating1 + weighted_ratings_sum / abs_weight_sum
            else:
                pred = avg_rating1
        elif isinstance(user1, int) and not isinstance(business, int):
            tmp = user_ratings_bc.value[user1].values()
            pred = sum(tmp) / len(tmp)
        elif not isinstance(user1, int) and isinstance(business, int):
            tmp = business_ratings_bc.value[business].values()
            pred = sum(tmp) / len(tmp)
        else:
            pred = global_avg_bc.value
        # lazy modification for prediction, rerange the prediction to [1, 5]
        if pred < 1:
            pred = 1
        elif pred > 5:
            pred = 5
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
        lambda lines: emit_rating_pairs_user_base(lines, uid2idx_bc, bid2idx_bc, key='business', method=2)).reduceByKey(
        lambda a, b: a + b).collectAsMap()
    business_uidx_bc = sc.broadcast(business_uidx)

    # {bidx: [rating1, rating2, ...],
    #  ...}
    business_ratings = train_data.mapPartitions(
        lambda lines: emit_rating_pairs_user_base(lines, uid2idx_bc, bid2idx_bc, key='business', method=1)).reduceByKey(
        lambda a, b: a + b).collectAsMap()
    business_ratings_bc = sc.broadcast(business_ratings)

    raw_test = sc.textFile(test_file_path, numPartitions)
    test_header = raw_test.first()
    # [uid,bid,str(score)]
    test_data = raw_test.filter(lambda x: x != test_header).mapPartitions(lambda x: csv.reader(x))
    # [uidx,bidx]
    test_pairs = test_data.mapPartitions(lambda lines: emit_pairs_user_base(lines, uid2idx_bc, bid2idx_bc))
    test_pairs.persist()
    # ['uid,bid,pred']
    test_pred_id_pairs = test_pairs.mapPartitions(
        lambda pairs: user_based_predict(pairs, user_ratings_bc, business_ratings_bc, user_bidxs_bc, business_uidx_bc,
                                         global_avg_bc, positive_sim=True)).mapPartitions(
        lambda lines: emit_id_pairs_user_base(lines, uidx2id_bc, bidx2id_bc)).collect()
    sc.stop()
    with open(output_file_path, 'w') as out_f:
        print("user_id, business_id, prediction", file=out_f)
        for line in test_pred_id_pairs:
            print(line, file=out_f)


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
        # print("User-based")
        user_based_CF(train_file_path, test_file_path, output_file_path, 3)
    elif case_id == 3:
        # Item-based CF
        pass
    else:
        raise ValueError("Case ID should be 1, 2, or 3")
