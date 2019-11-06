#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/25/19 1:25 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task1.py
# @Software: PyCharm

from pyspark import SparkContext
import sys
import csv
import random
from time import time
from itertools import combinations


def id_to_index(iteration, uid2idx_bc, bid2idx_bc, order="u,b"):
    for review in iteration:
        uidx = uid2idx_bc.value[review[0]]
        bidx = bid2idx_bc.value[review[1]]
        if order == "u,b":
            yield (uidx, [bidx])
        elif order == "b,u":
            yield (bidx, [uidx])


def sort_idxs(iteration):
    for line in iteration:
        yield (line[0], sorted(line[1]))


# [uidx, [h1(uidx), h2(uidx), ...]]
def cal_hash(iters, a_list, b_list, user_cnt):
    for x in iters:
        yield (x, [(x * a + b) % user_cnt for a, b in zip(a_list, b_list)])


def emit_sig_band(signature_cols, r, b):
    for sig_col in signature_cols:
        for i in range(b):
            band_cnt = str(i)
            partial_sig = sig_col[1][i * r:(i + 1) * r]
            sig_bucket = '_'.join([str(sig) for sig in partial_sig])
            sig_bucket = band_cnt + '_' + sig_bucket
            yield (sig_bucket, [sig_col[0]])  # emit (sig_bucket, bidx)


def emit_candidate_pairs(buckets):
    pairs = []
    for bucket in buckets:
        if len(bucket[1]) == 2:
            pair = ','.join([str(i) for i in sorted(bucket[1])])
            if pair not in pairs:
                pairs.append(pair)
                yield pair
        elif len(bucket[1]) > 2:
            for pair in combinations(bucket[1], 2):
                pair = ','.join([str(i) for i in sorted(pair)])
                if pair not in pairs:
                    pairs.append(pair)
                    yield pair


def min_hash(iters, hash_bc):
    hash_num = len(hash_bc.value[0])
    for business_col in iters:
        bidx = business_col[0]
        uidxs = business_col[1]
        tmp_signature_col = [[] for i in range(hash_num)]
        for uidx in uidxs:
            hash_col_u = hash_bc.value[uidx]
            for i in range(hash_num):
                tmp_signature_col[i].append(hash_col_u[i])
        signature_col = [min(i) for i in tmp_signature_col]  # a little bit faster than using list(map(min,tmp_signature_col))
        yield (bidx, signature_col)


def make_hash_params(hash_num, business_cnt):
    random.seed(42)
    a_values = []
    b_values = []
    for i in range(hash_num):
        a_r = random.randint(1, business_cnt)
        while a_r in a_values:
            a_r = random.randint(1, business_cnt)
        a_values.append(a_r)
        b_values.append(random.randint(1, business_cnt))
    return a_values, b_values


def cal_jaccard_sim(pairs, char_mat_bc):
    for pair in pairs:
        pair = [int(i) for i in pair.split(',')]
        s1 = set(char_mat_bc.value[pair[0]])
        s2 = set(char_mat_bc.value[pair[1]])
        similarity = len(s1.intersection(s2)) / float(len(s1.union(s2)))
        if similarity >= 0.5:
            yield [pair[0], pair[1], similarity]


def transform_to_bid_pair(similar_pairs, idx_to_bid_bc):
    for pair in similar_pairs:
        bid_pair = sorted([idx_to_bid_bc.value[pair[0]], idx_to_bid_bc.value[pair[1]]])
        yield [bid_pair[0], bid_pair[1], pair[2]]


def using_textFile(input_file, output_file, numPartitions):
    start = time()
    sc = SparkContext.getOrCreate()
    raw_data = sc.textFile(input_file, minPartitions=numPartitions)  # (input_file, minPartition)
    header = raw_data.first()
    clean_data = raw_data.filter(lambda x: x != header).mapPartitions(lambda x: csv.reader(x))

    user_ids = clean_data.map(lambda x: x[0]).distinct().collect()
    user_cnt = len(user_ids)
    business_ids = clean_data.map(lambda x: x[1]).distinct().collect()
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

    # convert to [uidx,[bidx1, bidx2, ...]], [bidx1, bidx2, ...] is sorted
    rows = clean_data.mapPartitions(lambda iters: id_to_index(iters, uid2idx_bc, bid2idx_bc, "u,b")).reduceByKey(
        lambda a, b: a + b).mapPartitions(
        lambda iters: sort_idxs(iters))
    rows.persist()  # TODO???

    # convert to [bidx,[uidx1, uidx2, ...]] , [uidx1, uidx2, ...] is sorted
    columns = clean_data.mapPartitions(lambda iters: id_to_index(iters, uid2idx_bc, bid2idx_bc, "b,u")).reduceByKey(
        lambda a, b: a + b).mapPartitions(
        lambda iters: sort_idxs(iters))
    columns.persist()  # TODO???
    columns_bc = sc.broadcast(columns.collectAsMap())

    # make hash parameters
    r, b = 3, 30
    hash_num = int(r * b)
    a_values, b_values = make_hash_params(hash_num, business_cnt)

    # {uidx1: [h1(uidx1), h2(uidx1), ...],
    #  uidx2: [h1(uidx2), h2(uidx2), ...],
    #  ...}
    hash_values = rows.keys().mapPartitions(lambda row_nums: cal_hash(row_nums, a_values, b_values, user_cnt)).collectAsMap()
    hash_values_bc = sc.broadcast(hash_values)

    # calculate min-hash signature matrix
    # (bidx, [sig1,sig2, ...])
    signature_mat = columns.mapPartitions(lambda iteration: min_hash(iteration, hash_values_bc))

    distinct_candidate_pair = signature_mat.mapPartitions(lambda signature_cols: emit_sig_band(signature_cols, r, b)).reduceByKey(
        lambda x, y: x + y).filter(lambda x: len(x[1]) > 1).mapPartitions(emit_candidate_pairs).distinct()
    similar_pair_idx = distinct_candidate_pair.mapPartitions(lambda pairs: cal_jaccard_sim(pairs, columns_bc))
    similar_pair_bid = similar_pair_idx.mapPartitions(lambda similar_pairs: transform_to_bid_pair(similar_pairs, idx2bid_bc)).sortBy(
        lambda pair: [pair[0], pair[1]])
    res_lines = similar_pair_bid.map(lambda x: x[0] + ',' + x[1] + ',%.2f' % x[2]).collect()
    with open(output_file, 'w') as out_f:
        print("business_id_1, business_id_2, similarity", file=out_f)
        for line in res_lines:
            print(line, file=out_f)
    sc.stop()
    end = time()
    duration = int(end - start)
    return duration, res_lines


def using_parallelize(input_file, output_file, numPartitions):
    start = time()
    sc = SparkContext.getOrCreate()
    # load review data
    raws = []
    with open(input_file, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raws.append(row)
    clean_data = sc.parallelize(raws, numPartitions)
    user_ids = clean_data.map(lambda x: x[0]).distinct().collect()
    user_cnt = len(user_ids)
    business_ids = clean_data.map(lambda x: x[1]).distinct().collect()
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

    # convert to [uidx,[bidx1, bidx2, ...]], [bidx1, bidx2, ...] is sorted
    rows = clean_data.mapPartitions(lambda iters: id_to_index(iters, uid2idx_bc, bid2idx_bc, "u,b")).reduceByKey(
        lambda a, b: a + b).mapPartitions(
        lambda iters: sort_idxs(iters))
    rows.persist()  # TODO???

    # convert to [bidx,[uidx1, uidx2, ...]] , [uidx1, uidx2, ...] is sorted
    columns = clean_data.mapPartitions(lambda iters: id_to_index(iters, uid2idx_bc, bid2idx_bc, "b,u")).reduceByKey(
        lambda a, b: a + b).mapPartitions(
        lambda iters: sort_idxs(iters))
    columns.persist()  # TODO???
    columns_bc = sc.broadcast(columns.collectAsMap())

    # make hash parameters
    r, b = 3, 30
    hash_num = int(r * b)
    a_values, b_values = make_hash_params(hash_num, business_cnt)

    # {uidx1: [h1(uidx1), h2(uidx1), ...],
    #  uidx2: [h1(uidx2), h2(uidx2), ...],
    #  ...}
    hash_values = rows.keys().mapPartitions(lambda row_nums: cal_hash(row_nums, a_values, b_values, user_cnt)).collectAsMap()
    hash_values_bc = sc.broadcast(hash_values)

    # calculate min-hash signature matrix
    # (bidx, [sig1,sig2, ...])
    signature_mat = columns.mapPartitions(lambda iteration: min_hash(iteration, hash_values_bc))

    distinct_candidate_pair = signature_mat.mapPartitions(lambda signature_cols: emit_sig_band(signature_cols, r, b)).reduceByKey(
        lambda x, y: x + y).filter(lambda x: len(x[1]) > 1).mapPartitions(emit_candidate_pairs).distinct()
    similar_pair_idx = distinct_candidate_pair.mapPartitions(lambda pairs: cal_jaccard_sim(pairs, columns_bc))
    similar_pair_bid = similar_pair_idx.mapPartitions(lambda similar_pairs: transform_to_bid_pair(similar_pairs, idx2bid_bc)).sortBy(
        lambda pair: [pair[0], pair[1]])
    res_lines = similar_pair_bid.map(lambda x: x[0] + ',' + x[1] + ',%.2f' % x[2]).collect()
    with open(output_file, 'w') as out_f:
        print("business_id_1, business_id_2, similarity", file=out_f)
        for line in res_lines:
            print(line, file=out_f)
    sc.stop()
    end = time()
    duration = int(end - start)
    return duration, res_lines


def get_res_str_pairs(res_lines):
    res = []
    for line in res_lines:
        line = line.split(',')
        # each pair as "business_id1,business_id2"
        res.append(','.join(line[:2]))
    return res


def get_ground_truth():
    truth_file = "/Users/liangsiqi/Documents/Dataset/yelp_rec_data/pure_jaccard_similarity.csv"
    sc = SparkContext.getOrCreate()
    raw_ground_truth = sc.textFile(truth_file)
    header = raw_ground_truth.first()
    ground_truth_lines = raw_ground_truth.filter(lambda x: x != header).mapPartitions(lambda x: csv.reader(x))
    ground_truth = ground_truth_lines.map(lambda x: x[0] + ',' + x[1]).collect()
    sc.stop()
    return ground_truth


def cal_precision_recall(res, ground_truth):
    in_ground_truth = 0
    for pair in res:
        if pair in ground_truth:
            in_ground_truth += 1
    precision = in_ground_truth / len(res)
    recall = in_ground_truth / len(ground_truth)
    return precision, recall


if __name__ == "__main__":
    argv = sys.argv
    input_file = argv[1]
    similarity_method = argv[2]
    output_file = argv[3]
    numPartitions = 5

    if similarity_method.lower() != "jaccard":
        raise ValueError("Similarity method '%s' is not implemented!" % similarity_method)
    using_parallelize(input_file, output_file, numPartitions)
