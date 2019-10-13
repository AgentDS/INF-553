#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/27/19 3:02 AM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task1.py
# @Software: PyCharm

from pyspark import SparkContext
import sys
import csv
from collections import Counter
from itertools import combinations


def candidate_count(baskets, candidate_itemsets):
    itemsets_count = []
    baskets = list(baskets)
    for basket in baskets:
        for itemset in candidate_itemsets:
            if all([item in basket for item in itemset.split('_')]):
                itemsets_count.append(itemset)
    return Counter(itemsets_count).items()


def A_priori(baskets, support, total_baskets_cnt):
    clean_baskets = list(baskets)
    local_support = support * len(clean_baskets) / float(total_baskets_cnt)
    max_itemset_size = max([len(basket) for basket in clean_baskets])
    # candidate_itemsets = {str(i): [] for i in range(1, max_itemset_size + 1)}
    candidate_itemsets = []

    # frequent items
    single_cnt = Counter()
    for basket in clean_baskets:
        single_cnt.update(basket)

    candidate_single = sorted([i for i in single_cnt if single_cnt[i] >= local_support])
    former_candidate = candidate_single
    # candidate_itemsets['1'].extend(former_candidate)
    candidate_itemsets.extend(former_candidate)

    # frequent itemsets with size >= 2
    for itemset_size in range(2, max_itemset_size + 1):
        tmp_itemsets = []
        for basket in clean_baskets:
            if len(basket) < itemset_size:
                continue
            for itemset in combinations(basket, itemset_size):
                itemset = sorted(itemset)
                if itemset_size == 2:
                    immediate_subset = [i[0] for i in combinations(itemset, itemset_size - 1)]
                else:
                    immediate_subset = [sorted(i) for i in combinations(itemset, itemset_size - 1)]
                immediate_subset_is_candidate = [i in former_candidate for i in immediate_subset]
                if all(immediate_subset_is_candidate):
                    tmp_itemsets.append('_'.join(itemset))
        itemset_cnt = Counter(tmp_itemsets)
        # former_candidate = [i.split('_') for i in itemset_cnt if itemset_cnt[i] >= local_support]
        # keep itemset in form of id1_id2_id3 for sake of later hash
        former_candidate = [i for i in itemset_cnt if itemset_cnt[i] >= local_support]
        # candidate_itemsets[str(itemset_size)].extend(former_candidate)
        candidate_itemsets.extend(former_candidate)

    return candidate_itemsets


if __name__ == "__main__":
    argv = sys.argv
    case_number = int(argv[1])
    support = int(argv[2])
    input_file = argv[3]

    input_file = "../data/small1.csv"

    sc = SparkContext()
    raw_data = sc.textFile(input_file)
    header = raw_data.first()
    raw_data_without_header = raw_data.filter(lambda x: x != header)
    clean_data = raw_data_without_header.map(lambda line: [i for i in line.strip().split(',')])

    # whether there is faster method??
    if case_number == 1:
        baskets = clean_data.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b).map(
            lambda x: [x[0], sorted(list(set(list(x[1]))))])
    else:
        baskets = clean_data.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda a, b: a + b).map(
            lambda x: [x[0], sorted(list(set(list(x[1]))))])
    # get rid of original key:
    #   case 1 only remain [business_ids];
    #   case 2 only remain [user_ids]
    clean_baskets = baskets.map(lambda x: x[1])
    clean_baskets.persist()
    total_baskets_cnt = clean_baskets.count()

    # Phase 1: get candidate itemsets from all subsets (different partitions)
    phase1_map = clean_baskets.mapPartitions(lambda baskets_subset: A_priori(baskets_subset, support, total_baskets_cnt)).map(
        lambda x: (x, 1))
    phase1_reduce = phase1_map.reduceByKey(lambda x, y: 1).keys().collect()  # remove duplicates, Intermediate result

    # phase 2: count candidate itemsets and filter out using support
    phase2_map = clean_baskets.mapPartitions(lambda baskets_subset: candidate_count(baskets_subset, phase1_reduce))
    phase2_reduce = phase2_map.reduceByKey(lambda x, y: x + y).filter(lambda kv: kv[1] >= support).keys().map(
        lambda x: x.split('_')).collect()
