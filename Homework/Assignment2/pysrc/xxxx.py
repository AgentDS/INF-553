#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/12/19 6:05 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : xxxx.py
# @Software: PyCharm
import csv
import random
from collections import Counter
from itertools import combinations

input_file = "../data/small1.csv"
support = 4

raw_data = []  # user_id,business_id
with open(input_file, 'r') as input:
    readCSV = csv.reader(input, delimiter=',')
    cnt = 0
    for row in readCSV:
        cnt += 1
        if cnt > 1:
            raw_data.append(row)

baskets = dict()  # user_id: [business_id1, business_id2, business_id3, ...]
for row in raw_data:
    if row[0] not in baskets:
        baskets[row[0]] = []
    baskets[row[0]].append(row[1])

for user in baskets:
    basket = baskets[user]
    baskets[user] = sorted(basket)
clean_baskets = [baskets[i] for i in baskets]

single_cnt = Counter()
for basket in clean_baskets:
    single_cnt.update(basket)

max_itemset_size = max([len(basket) for basket in clean_baskets])

candidate_single = sorted([i for i in single_cnt if single_cnt[i] >= support])
former_candidate = candidate_single
print(candidate_single)

candidate_itemsets = {str(i): [] for i in range(1, max_itemset_size + 1)}
candidate_itemsets['1'].extend(former_candidate)

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
    former_candidate = [i.split('_') for i in itemset_cnt if itemset_cnt[i] >= support]
    candidate_itemsets[str(itemset_size)].extend(former_candidate)