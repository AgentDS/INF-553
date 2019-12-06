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
    return zero_nums


def find_median(x):
    n = len(x)
    x = sorted(x)
    if n % 2 == 0:
        # print("len of x: ", n)
        median1 = x[int(n / 2)]
        median2 = x[int(n / 2) - 1]
        median = (median1 + median2) / 2
    else:
        median = x[n // 2]
    return median


def average_median_R(R_powers, group_num):
    num_each_group = int(len(R_powers) / group_num)
    # print("num_each_group:", num_each_group)
    medians = []
    for i in range(group_num):
        start_idx = i * num_each_group
        end_idx = (i + 1) * num_each_group
        within_group = R_powers[start_idx:end_idx]
        # print("R_power: ", R_powers)
        # print("len of within group: ", len(within_group))
        # print(R_powers[start_idx:end_idx])
        medians.append(find_median(within_group))
    return int(sum(medians) / len(medians))


def median_average_R(R_powers, group_num):
    num_each_group = int(len(R_powers) / group_num)
    avgs = []
    for i in range(group_num):
        start_idx = group_num * num_each_group
        end_idx = (group_num + 1) * num_each_group
        within_group = R_powers[start_idx:end_idx]
        avgs.append(sum(within_group) / num_each_group)
    return int(find_median(avgs))


def process_stream(rdd, file):
    cities_within_window = rdd.collect()
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
    ground_truth = len(set(cities_within_window))

    print("{0},{1},{2}".format(datetime.fromtimestamp(int(time())), ground_truth, estimate), file=file)


if __name__ == "__main__":
    argv = sys.argv
    port_num = int(argv[1])
    output_filename = argv[2]

    random.seed(0)
    hash_num_each_group = 8
    hash_group = 16
    m = 401
    timestamp_hist = []
    ground_truth_hist = []
    estimate_hist = []

    with open(output_filename, 'w') as out_f:
        print("Time,Ground Truth,Estimation", file=out_f)
        sc = SparkContext.getOrCreate()
        ssc = StreamingContext(sc, 5)
        lines = ssc.socketTextStream("localhost", port_num)
        state_stream = lines.transform(lambda rdd: rdd.map(json.loads).map(lambda x: x['city']))
        city_window = state_stream.window(30, 10)
        city_window.foreachRDD(lambda rdd: process_stream(rdd, out_f))

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
