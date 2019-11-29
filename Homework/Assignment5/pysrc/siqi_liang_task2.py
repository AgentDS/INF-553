#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/25/19 10:36 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task2.py
# @Software: PyCharm
from pyspark import SparkContext
import json
import random
import binascii
import math
import sys
from collections import Counter
from pyspark.streaming import StreamingContext
from time import time
from datetime import datetime


def hash_maker(hash_num, m):
    max_num = 3000
    a_values = []
    b_values = []
    for i in range(hash_num):
        a_r = random.randint(1, max_num)
        while a_r in a_values or math.gcd(a_r, m) != 1:
            a_r = random.randint(1, max_num)
        a_values.append(a_r)
        b_values.append(random.randint(1, max_num))
    return a_values, b_values


def zero_trail_num(hash_values):
    zero_nums = []
    for v in hash_values:
        bin_v = bin(v)
        zero_nums.append(len(bin_v) - len(bin_v.rstrip('0')))


def find_median(x):
    n = len(x)
    x = sorted(x)
    if n % 2 == 0:
        median1 = x[n // 2]
        median2 = x[n // 2 - 1]
        median = (median1 + median2) / 2
    else:
        median = x[n // 2]
    return median


def average_median_R(R_powers, group_num):
    num_each_group = int(len(R_powers) / group_num)
    medians = []
    for i in range(group_num):
        start_idx = group_num * num_each_group
        end_idx = (group_num + 1) * num_each_group
        within_group = R_powers[start_idx:end_idx]
        medians.append(find_median(within_group))
    return sum(medians) / len(medians)


def median_average_R(R_powers, group_num):
    num_each_group = int(len(R_powers) / group_num)
    avgs = []
    for i in range(group_num):
        start_idx = group_num * num_each_group
        end_idx = (group_num + 1) * num_each_group
        within_group = R_powers[start_idx:end_idx]
        avgs.append(sum(within_group) / num_each_group)
    return find_median(avgs)


def process_stream(rdd):
    cities_within_window = rdd.collect()
    hash_num_each_group = 6
    hash_group = 4
    m = 400
    max_zero_num = [0 for i in range(hash_num_each_group * hash_group)]
    a_values, b_values = hash_maker(hash_num_each_group * hash_group, m)

    for city in cities_within_window:
        int_city = int(binascii.hexlify(city.encode('utf8')), 16)
        hash_values = [(a * int_city + b) % m for a, b in zip(a_values, b_values)]
        trailing_zero_num = zero_trail_num(hash_values)
        for i in range(hash_num_each_group * hash_group):
            if trailing_zero_num[i] > max_zero_num[i]:
                max_zero_num[i] = trailing_zero_num[i]

    R_power_values = [2 ** i for i in max_zero_num]
    estimate = average_median_R(R_power_values, hash_group)

    print(len(set(city)))
    print(len(set(city)))
    print(len(set(city)))
    print(len(set(city)))
    print(len(set(city)))
    print(len(set(city)))
    print(len(set(city)))
    print(len(set(city)))
    print(len(set(city)))
    print(len(set(city)))


if __name__ == "__main__":
    # argv = sys.argv
    # port_num = int(argv[1])
    # output_filename = argv[2]

    random.seed(0)
    unique_states = set()
    global_counter = Counter()

    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 5)
    lines = ssc.socketTextStream("localhost", 9999)
    state_stream = lines.transform(lambda rdd: rdd.map(json.loads).map(lambda x: x['city']))
    # state_stream.foreachRDD(process_stream)
    city_window = state_stream.window(30, 10)
    city_window.foreachRDD(lambda rdd: process_stream(rdd))

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

    # with open(output_filename, 'w') as out_f:
    #     print("Time,Ground Truth,Estimation", file=out_f)
    #     for i in range(len(fpr_hist)):
    #         print("{0},{1}".format(datetime.fromtimestamp(timestamp_hist[i]), fpr_hist[i]), file=out_f)
