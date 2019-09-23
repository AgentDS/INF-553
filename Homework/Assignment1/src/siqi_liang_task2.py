#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-09-14 01:43
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task2.py
# @Software: PyCharm

import sys
import json
from pyspark import SparkContext
from time import time


def iterm_num(iterator):
    cnt = 0
    for x in iterator:
        cnt += 1
    yield cnt


def sorted_within_partition(iterator):
    yield sorted(iterator, key=lambda x: -x[1], reverse=False)


def top10_value(iterator):
    iterms_list = list(iterator)[0]
    if len(iterms_list) < 10:
        yield iterms_list
    else:
        yield iterms_list[:10]


if __name__ == '__main__':
    argv = sys.argv
    user_file = argv[1]
    output_file = argv[2]
    customized_n_partition = int(argv[3])

    # use default partition number
    sc = SparkContext()
    start1 = time()
    userRDD_default = sc.textFile(user_file)
    default_n_partition = userRDD_default.getNumPartitions()
    default_n_items = userRDD_default.mapPartitions(iterm_num).collect()
    userRDD_json_default = userRDD_default.map(json.loads)
    review_cnt_RDD = userRDD_json_default.map(lambda x: [x['user_id'], x['review_count']])
    most_review = review_cnt_RDD.sortBy(lambda x: x[1], ascending=False).take(10)
    end1 = time()
    default_exe_time = end1 - start1
    sc.stop()

    # use given partition number
    sc.stop()
    sc = SparkContext()
    start2 = time()
    userRDD_customized = sc.textFile(user_file, customized_n_partition)
    # print("Customized partition number: %d" % userRDD_customized.getNumPartitions())
    customized_n_items = userRDD_customized.mapPartitions(iterm_num).collect()
    review_cnt_RDD_customized = userRDD_customized.map(json.loads).map(lambda x: [x['user_id'], x['review_count']])
    # print(review_cnt_RDD_customized.getNumPartitions())
    sorted_within_partitionRDD = review_cnt_RDD_customized.mapPartitions(sorted_within_partition)
    top10_within_partitionRDD = sorted_within_partitionRDD.mapPartitions(top10_value)
    top10_reviews = top10_within_partitionRDD.flatMap(lambda x: x).takeOrdered(10, key=lambda x: -x[1])
    end2 = time()
    print(top10_reviews)
    customized_exe_time = end2 - start2
    # print("Time for customized %d partitions: %d s" % (customized_n_partition, customized_exe_time))

    explanation = ['Without customized partition functions, the program ',
                   'tries to sort all 1637138 users first then get the ',
                   'top-10 users; while using customized partition ',
                   'functions, the program first sorted within each ',
                   'partition and get top-10 for each partition, then ',
                   'sort  10 x n_partition  users  and output the final top-10 ',
                   'result. The default method takes much longer time ',
                   'as well as takes more memory for storing intermediate ',
                   '(Key,Value) results.']
    explanation = ''.join(explanation)

    task2_json = {
        "default": {"n_partition": default_n_partition, "n_items": default_n_items, "exe_time": default_exe_time},
        "customized": {"n_partition": customized_n_partition, "n_items": customized_n_items,
                       "exe_time": customized_exe_time},
        "explanation": explanation
    }
    with open(output_file, "w") as write_file:
        json.dump(task2_json, write_file)
