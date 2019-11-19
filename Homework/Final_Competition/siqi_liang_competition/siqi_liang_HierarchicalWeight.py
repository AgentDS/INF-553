#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/18/19 8:41 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_HierarchicalWeight.py
# @Software: PyCharm

import sys
from pyspark import SparkContext, SparkConf
import csv
from time import time


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


def rating_dict(rating_lines):
    for line in rating_lines:
        yield (line[0], dict(line[1]))


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


def avg_predict(pairs, user_ratings_bc, business_ratings_bc, user_bidxs_bc, business_uidx_bc, global_avg_bc):
    for pair in pairs:
        user1 = pair[0]
        business = pair[1]
        if isinstance(user1, int) and isinstance(business, int):
            tmp = user_ratings_bc.value[user1].values()
            user1_avg_rating = sum(tmp) / len(tmp)

            # todo: prefer item_avg
            tmp = business_ratings_bc.value[business]
            business_avg = sum(tmp) / len(tmp)
            pred = (user1_avg_rating + business_avg) / 2
        elif isinstance(user1, int) and not isinstance(business, int):
            tmp = user_ratings_bc.value[user1].values()
            pred = sum(tmp) / len(tmp)
        elif not isinstance(user1, int) and isinstance(business, int):
            tmp = business_ratings_bc.value[business]
            pred = sum(tmp) / len(tmp)
        else:
            pred = global_avg_bc.value

        yield [user1, business, pred]


def global_AVG_method(train_file_path, test_file_path, output_file_path, numPartitions):
    conf = SparkConf().set('spark.driver.host', '127.0.0.1')
    sc = SparkContext(master='local', appName='myAppName', conf=conf)

    raw_train = sc.textFile(train_file_path, numPartitions)
    header = raw_train.first()
    train_data = raw_train.filter(lambda x: x != header).mapPartitions(lambda x: csv.reader(x))

    global_avg = train_data.map(lambda x: float(x[2])).mean()
    global_avg_bc = sc.broadcast(global_avg)

    user_ids = train_data.map(lambda x: x[0]).distinct().collect()
    business_ids = train_data.map(lambda x: x[1]).distinct().collect()

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
        lambda pairs: avg_predict(pairs, user_ratings_bc, business_ratings_bc, user_bidxs_bc, business_uidx_bc,
                                  global_avg_bc)).mapPartitions(
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


def error_distribution(output_file_path, test_file_path):
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
    err_dict = ground_val_pair.join(predict_val_pair).map(lambda x: abs(x[1][0] - x[1][1])).map(lambda x: (int(x), 1)).reduceByKey(
        lambda a, b: a + b).collectAsMap()
    for i in range(5):
        if i != 4:
            if i in err_dict:
                print(">=%d and <%d: %d" % (i, i + 1, err_dict[i]))
            else:
                print(">=%d and <%d: %d" % (i, i + 1, 0))
        else:
            if i in err_dict:
                print(">=4: %d" % err_dict[i])
            else:
                print(">=4: 0")
    sc.stop()


if __name__ == "__main__":
    prefix = "/Users/liangsiqi/Documents/Dataset/inf553final_competition/customized/"
    log_path = "./avg_method_log.txt"
    rmse_hist = []
    duration_hist = []
    print("train-val        duration         RMSE")
    for i in range(1, 5):
        train_path = prefix + "subtrain%d.csv" % i
        test_path = prefix + "subval%d.csv" % i
        start = time()
        global_AVG_method(train_path, test_path, prefix + "./pred%d.csv" % i, 4)
        end = time()
        rmse = cal_RMSE(prefix + "./pred%d.csv" % i, test_path)
        duration = int(end - start)
        rmse_hist.append(rmse)
        duration_hist.append(duration)
        print("{0:d}                {1:3d}s             {2:.10f}".format(i, duration, rmse))
    train_path = "/Users/liangsiqi/Documents/Dataset/inf553final_competition/yelp_train.csv"
    test_path = "/Users/liangsiqi/Documents/Dataset/inf553final_competition/yelp_val.csv"
    start = time()
    global_AVG_method(train_path, test_path, "/Users/liangsiqi/Documents/Dataset/inf553final_competition/" + "val_pred.csv", 4)
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
