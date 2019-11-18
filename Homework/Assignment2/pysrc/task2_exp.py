#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/17/19 1:58 AM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : task2_exp.py
# @Software: PyCharm

from pyspark import SparkContext
import sys
import csv
from time import time
from collections import Counter
from itertools import combinations


def get_customer_count(iterations):
    for pair in iterations:
        date_customer = pair[0].split('-')
        yield (date_customer[1], 1)


def emit_candidate_pairs(pairs, candidate_customers_bc):
    for pair in pairs:
        date_customer = pair[0].split('-')
        if date_customer[1] in candidate_customers_bc.value:
            yield (pair[0], [pair[1]])


def extract_date_customer_product(iterator):
    pairs = []
    for line in iterator:
        pairs.append([line[0] + '-' + line[1], int(line[5])])
    return pairs


def sort_into_file(reduce_result, tag, file=None):
    inter_result = [i.split('_') for i in reduce_result]
    max_size = max([len(i) for i in inter_result])
    candidate_itemsets = {i: [] for i in range(1, max_size + 1)}
    [candidate_itemsets[len(i)].append(i) for i in inter_result]

    for i in range(1, max_size + 1):
        itemsets_size_i = [tuple(x) for x in candidate_itemsets[i]]
        candidate_itemsets[i] = sorted(itemsets_size_i, key=lambda x: [i for i in x])  # sorted in lexicographical order

    print(tag, file=file)
    for i in range(1, max_size + 1):
        itemsets = candidate_itemsets[i]
        if i == 1:
            set_cnt = len(itemsets)
            for j in range(set_cnt):
                if j < set_cnt - 1:
                    print("('%s')" % itemsets[j], end=",", file=file)
                else:
                    print("('%s')" % itemsets[j], file=file)
        else:
            set_cnt = len(itemsets)
            for j in range(set_cnt):
                if j < set_cnt - 1:
                    print(itemsets[j], end=",", file=file)
                else:
                    print(itemsets[j], file=file)
        if i != max_size:
            print("", file=file)


def candidate_count(baskets, candidate_itemsets):
    itemsets_count = Counter()
    baskets = list(baskets)
    for basket in baskets:
        for itemset in candidate_itemsets:
            if all([item in basket for item in itemset.split('_')]):
                itemsets_count.update([itemset])
    return itemsets_count.items()


def A_priori_long_basket(baskets, support, total_baskets_cnt):
    clean_baskets = list(baskets)
    local_support = support * len(clean_baskets) / float(total_baskets_cnt)
    baskets_cnt_ordered = sorted(Counter([len(basket) for basket in clean_baskets]).items(), reverse=True)
    # Reduce max_itemset_size, if the number of baskets with certain length is less than local_support,
    # then the maximum size of frequent itemset must be smaller than the length of such baskets
    acc_sum = 0
    for cnt_item in baskets_cnt_ordered:
        max_itemset_size = cnt_item[0]
        acc_sum += cnt_item[1]
        if acc_sum >= local_support:
            break
        else:
            continue
    candidate_itemsets = []

    # frequent items
    single_cnt = Counter()
    for basket in clean_baskets:
        single_cnt.update(basket)

    candidate_single = sorted([i for i in single_cnt if single_cnt[i] >= local_support])
    former_candidate = candidate_single
    candidate_itemsets.extend(former_candidate)

    # frequent pairs
    itemset_cnt = Counter()
    former_possible_items_set = former_candidate
    for pair in combinations(former_possible_items_set, 2):
        pair = sorted(pair)
        for basket in clean_baskets:
            contain_pair_flag = True
            if len(basket) < 2:
                continue
            for item in pair:
                if item not in basket:
                    contain_pair_flag = False
                    break
            if contain_pair_flag:
                itemset_cnt.update(['_'.join(pair)])
    former_candidate = [i for i in itemset_cnt if itemset_cnt[i] >= local_support]
    candidate_itemsets.extend(former_candidate)
    if len(former_candidate) < 2:
        return candidate_itemsets

    # frequent itemsets with size >= 3
    for itemset_size in range(3, max_itemset_size + 1):
        itemset_cnt = Counter()
        possible_item_set = []
        [possible_item_set.extend(i.split('_')) for i in former_candidate]
        possible_item_set = list(set(possible_item_set))
        itemsets_checked = []
        for smaller_set_string in former_candidate:
            smaller_set = smaller_set_string.split('_')
            possible_to_add = set(possible_item_set) - set(smaller_set)
            for item_to_add in possible_to_add:
                new_itemset = sorted(smaller_set + [item_to_add])
                new_itemset_string = '_'.join(new_itemset)
                if new_itemset_string not in itemsets_checked:
                    itemsets_checked.append(new_itemset_string)
                    immediate_subset = ['_'.join(sorted(i)) for i in combinations(new_itemset, itemset_size - 1)]
                    all_subset_in_former = all([i in former_candidate for i in immediate_subset])
                    if all_subset_in_former:
                        for basket in clean_baskets:
                            if all([i in basket for i in new_itemset]):
                                itemset_cnt.update([new_itemset_string])
        former_candidate = [i for i in itemset_cnt if itemset_cnt[i] >= local_support]
        candidate_itemsets.extend(former_candidate)
        if len(former_candidate) < 2:
            break

    return candidate_itemsets


def A_priori_short_basket(baskets, support, total_baskets_cnt):
    clean_baskets = list(baskets)
    local_support = support * len(clean_baskets) / float(total_baskets_cnt)
    baskets_cnt_ordered = sorted(Counter([len(basket) for basket in clean_baskets]).items(), reverse=True)
    # Reduce max_itemset_size, if the number of baskets with certain length is less than local_support,
    # then the maximum size of frequent itemset must be smaller than the length of such baskets
    acc_sum = 0
    for cnt_item in baskets_cnt_ordered:
        max_itemset_size = cnt_item[0]
        acc_sum += cnt_item[1]
        if acc_sum >= local_support:
            break
        else:
            continue
    candidate_itemsets = []

    # frequent items
    single_cnt = Counter()
    for basket in clean_baskets:
        single_cnt.update(basket)

    candidate_single = sorted([i for i in single_cnt if single_cnt[i] >= local_support])
    former_candidate = candidate_single
    candidate_itemsets.extend(former_candidate)

    # frequent itemsets with size >= 2
    for itemset_size in range(2, max_itemset_size + 1):
        itemset_cnt = {}
        for basket in clean_baskets:
            if len(basket) < itemset_size:
                continue
            for itemset in combinations(basket, itemset_size):
                itemset = sorted(itemset)
                if itemset_size == 2:
                    immediate_subset_flag = True
                    for immediate_subset in combinations(itemset, itemset_size - 1):
                        if immediate_subset[0] not in former_candidate:
                            immediate_subset_flag = False
                            break
                else:
                    immediate_subset_flag = True
                    for immediate_subset in combinations(itemset, itemset_size - 1):
                        if '_'.join(sorted(immediate_subset)) not in former_candidate:
                            immediate_subset_flag = False
                            break
                if immediate_subset_flag:
                    itemset_string = '_'.join(itemset)
                    if itemset_string not in itemset_cnt:
                        itemset_cnt[itemset_string] = 0
                    itemset_cnt[itemset_string] += 1
        # keep itemset in form of id1_id2_id3 for sake of later hash
        former_candidate = [i for i in itemset_cnt if itemset_cnt[i] >= local_support]
        candidate_itemsets.extend(former_candidate)

    return candidate_itemsets


def run_all(minPartition, method="long"):
    filter_threshold = 20
    support = 50
    input_file = "/Users/liangsiqi/Documents/Dataset/ta_feng_dataset/ta_feng_all_months_merged.csv"
    output_file = "./local_task2.csv"
    pair_out_file = "./Customer_product.csv"

    # part 1: clean raw data file to (DATE-CUSTOMER_ID,PRODUCT_ID) pairs and write into file
    sc = SparkContext.getOrCreate()
    raw_data = sc.textFile(input_file, minPartition)
    header = raw_data.first()
    raw_data_without_header = raw_data.filter(lambda x: x != header)
    # "TRANSACTION_DT","CUSTOMER_ID","AGE_GROUP","PIN_CODE","PRODUCT_SUBCLASS","PRODUCT_ID","AMOUNT","ASSET","SALES_PRICE"
    clean_raw_data = raw_data_without_header.mapPartitions(lambda x: csv.reader(x))
    preprocessed_result = clean_raw_data.mapPartitions(lambda x: extract_date_customer_product(x)).collect()
    with open(pair_out_file, 'w') as out_f:
        print("DATE-CUSTOMER_ID,PRODUCT_ID", file=out_f)
        for pair in preprocessed_result:
            print(pair[0] + ',' + '%d' % pair[1], file=out_f)
    sc.stop()

    minPartition = 6
    sc = SparkContext.getOrCreate()
    raw_data = sc.textFile(pair_out_file, minPartition)
    header = raw_data.first()
    raw_data_without_header = raw_data.filter(lambda x: x != header)
    # DATE-CUSTOMER_ID,PRODUCT_ID
    clean_data = raw_data_without_header.mapPartitions(lambda x: csv.reader(x))
    date_customer_pairs = clean_data.mapPartitions(lambda pairs: get_customer_count(pairs)).reduceByKey(lambda a, b: a + b)
    candidate_customers = date_customer_pairs.filter(lambda x: x[1] > filter_threshold).map(lambda x: x[0]).collect()
    candidate_customers_bc = sc.broadcast(candidate_customers)
    baskets = clean_data.mapPartitions(lambda pairs: emit_candidate_pairs(pairs, candidate_customers_bc)).reduceByKey(lambda a, b: a + b)
    clean_baskets = baskets.map(lambda x: sorted(list(set(list(x[1])))))
    clean_baskets.persist()
    total_baskets_cnt = clean_baskets.count()

    # phase 1
    if method == 'long':
        phase1_map = clean_baskets.mapPartitions(lambda subset: A_priori_long_basket(subset, support, total_baskets_cnt)).map(
            lambda x: (x, 1))
    elif method == 'short':
        phase1_map = clean_baskets.mapPartitions(lambda subset: A_priori_short_basket(subset, support, total_baskets_cnt)).map(
            lambda x: (x, 1))
    else:
        raise ValueError("method could only be 'long' or 'short'!")
    phase1_reduce = phase1_map.reduceByKey(lambda x, y: 1).keys().collect()

    # phase 2
    phase2_map = clean_baskets.mapPartitions(lambda baskets_subset: candidate_count(baskets_subset, phase1_reduce))
    phase2_reduce = phase2_map.reduceByKey(lambda x, y: x + y).filter(lambda kv: kv[1] >= support).keys().collect()

    # output to file
    with open(output_file, 'w') as f:
        sort_into_file(phase1_reduce, "Candidates:", f)
        print("", file=f)
        sort_into_file(phase2_reduce, "Frequent Itemsets:", f)

    sc.stop()


if __name__ == '__main__':
    log_file = "./task2_local_log.txt"
    numPartitions = [50, 70]
    methods = ['long']
    duration_hist_long = []
    print("numPartition        method      duration")

    for minPartition in numPartitions:
        start = time()
        run_all(minPartition, 'long')
        duration = int(time() - start)
        print("{0:2d}                  {1}       {2:4d}s".format(minPartition, 'long', duration))
        duration_hist_long.append(duration)

    with open(log_file, 'w') as log_f:
        print("numPartition        method      duration", file=log_f)
        for i in range(len(numPartitions)):
            print("{0:2d}                  {1}       {2:4d}s".format(numPartitions[i], 'long', duration_hist_long[i]), file=log_f)
