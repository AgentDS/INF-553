#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/27/19 3:02 AM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task2.py
# @Software: PyCharm

from pyspark import SparkContext
import csv
import sys
from time import time
from collections import Counter
from itertools import combinations


def date_customer_product_pair(pairs, candidate_customers_bc):
    for pair in pairs:
        date_customer = pair[0].split('-')
        if date_customer[1] in candidate_customers_bc.value:
            yield (pair[0], [pair[1]])


def get_customer_product(iterations):
    for pair in iterations:
        date_customer = pair[0].split('-')
        yield (date_customer[1], [pair[1]])


def customer_filter(customer_infos, threshold=20):
    for customer_info in customer_infos:
        if len(set(customer_info[1])) > threshold:
            yield customer_info[0]


def partition_basket_cnt(baskets):
    local_cnt = []
    for basket in baskets:
        local_cnt.append(len(basket))
    yield local_cnt


def count_singleton(baskets):
    single_cnt = Counter()
    baskets_cnt = 0
    for basket in baskets:
        single_cnt.update(basket)
        baskets_cnt += 1
    single_cnt = list(single_cnt.items())
    for item in single_cnt:
        yield item


def count_pairs(baskets, singleton_bc):
    pair_cnt = Counter()
    current_pairs = []
    for basket in baskets:
        for pair in combinations(basket, 2):
            current_pairs.append('_'.join(sorted(pair)))
    pair_cnt.update(current_pairs)
    pair_cnt = list(pair_cnt.items())
    for item in pair_cnt:
        yield item


def extract_date_customer_product(iterator):
    pairs = []
    for line in iterator:
        pairs.append([line[0] + '-' + line[1], int(line[5])])
    return pairs


def combiner(candidates):
    exist = []
    for item in candidates:
        if item[0] not in exist:
            exist.append(item[0])
            yield item


def sort_into_file(frequent_singleton, frequent_pairs, reduce_result, tag, file=None):
    inter_pairs = [i.split('_') for i in frequent_pairs]
    inter_result = [i.split('_') for i in reduce_result]
    max_size = max([len(i) for i in inter_result])
    candidate_itemsets = {i: [] for i in range(1, max_size + 1)}
    [candidate_itemsets[1].append([i]) for i in frequent_singleton]
    [candidate_itemsets[2].append(i) for i in inter_pairs]
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
                    print("('{0}')".format(itemsets[j][0]), end=",", file=file)
                else:
                    print("('{0}')".format(itemsets[j][0]), file=file)
        else:
            set_cnt = len(itemsets)
            for j in range(set_cnt):
                if j < set_cnt - 1:
                    print(itemsets[j], end=",", file=file)
                else:
                    print(itemsets[j], file=file)
        if i != max_size:
            print("", file=file)


def A_priori_long_basket(baskets, support, total_baskets_cnt, frequent_pairs_bc):
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

    former_candidate = frequent_pairs_bc.value

    # frequent itemsets with size >= 3
    if max_itemset_size > 6:
        max_itemset_size = 6
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


def candidate_count(baskets, candidate_itemsets):
    itemsets_count = []
    baskets = list(baskets)
    for basket in baskets:
        for itemset in candidate_itemsets:
            if all([item in basket for item in itemset.split('_')]):
                itemsets_count.append(itemset)
    return Counter(itemsets_count).items()


if __name__ == "__main__":
    argv = sys.argv
    filter_threshold = int(argv[1])
    support = int(argv[2])
    input_file = argv[3]
    output_file = argv[4]
    pair_out_file = "./Customer_product.csv"
    start = time()

    # part 1: clean raw data file to (DATE-CUSTOMER_ID,PRODUCT_ID) pairs and write into file
    sc = SparkContext.getOrCreate()
    raw_data = sc.textFile(input_file, 3)
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

    sc = SparkContext.getOrCreate()
    raw_data = sc.textFile(pair_out_file, 3)
    header = raw_data.first()
    raw_data_without_header = raw_data.filter(lambda x: x != header)
    # DATE-CUSTOMER_ID,PRODUCT_ID
    clean_data = raw_data_without_header.mapPartitions(lambda x: csv.reader(x))
    clean_baskets_step1 = clean_data.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b).map(
        lambda x: sorted(list(set(x[1])))).filter(lambda x: len(x) > filter_threshold)

    clean_baskets_step1.persist()
    total_baskets_cnt = clean_baskets_step1.count()

    frequent_singleton = clean_baskets_step1.mapPartitions(lambda baskets: count_singleton(baskets)).reduceByKey(lambda a, b: a + b).filter(
        lambda x: x[1] >= support).keys().collect()
    singleton_bc = sc.broadcast(frequent_singleton)
    clean_baskets_step2 = clean_baskets_step1.filter(lambda x: len(x) > 1)
    clean_baskets_step2.persist()

    frequent_pairs = clean_baskets_step2.mapPartitions(lambda baskets: count_pairs(baskets, singleton_bc)).reduceByKey(
        lambda a, b: a + b).filter(lambda x: x[1] >= support).keys().collect()
    frequent_pairs_bc = sc.broadcast(frequent_pairs)
    clean_baskets_step3 = clean_baskets_step2.filter(lambda x: len(x) > 2)
    clean_baskets_step3.persist()

    # phase 1
    phase1_map = clean_baskets_step3.mapPartitions(
        lambda subset: A_priori_long_basket(subset, support, total_baskets_cnt, frequent_pairs_bc)).map(lambda x: (x, 1))
    phase1_reduce = phase1_map.mapPartitions(lambda candidates: combiner(candidates)).reduceByKey(lambda x, y: 1).keys().collect()
    # phase1_reduce = phase1_map.reduceByKey(lambda x, y: 1).keys().collect()

    # phase 2: count candidate itemsets and filter out using support
    phase2_map = clean_baskets_step3.mapPartitions(lambda baskets_subset: candidate_count(baskets_subset, phase1_reduce))
    phase2_reduce = phase2_map.reduceByKey(lambda x, y: x + y).filter(lambda kv: kv[1] >= support).keys().collect()

    # output to file
    with open(output_file, 'w') as f:
        sort_into_file(frequent_singleton, frequent_pairs, phase1_reduce, "Candidates:", f)
        print("", file=f)
        sort_into_file(frequent_singleton, frequent_pairs, phase2_reduce, "Frequent Itemsets:", f)

    sc.stop()

    end = time()
    print("Duration: %d" % int(end - start))
