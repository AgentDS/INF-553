#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/25/19 10:36 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task1.py
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
    random.seed(42)
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


def process_stream(rdd):
    stream = rdd.collect()
    for state in stream:
        int_state = int(binascii.hexlify(state.encode('utf8')), 16)
        hash_value = [(int_state * a + b) % bit_array_len for a, b in zip(a_values, b_values)]
        appearance_cnt = 0
        for h in hash_value:
            if global_bit_array[h] is False:
                global_bit_array[h] = True
            else:
                appearance_cnt += 1
        if appearance_cnt == hash_num:
            already_appear_pred = True
        else:
            already_appear_pred = False
        if int_state not in unique_states and not already_appear_pred:
            global_counter.update(['TN'])
        if int_state not in unique_states and already_appear_pred:
            global_counter.update(['FP'])
        unique_states.add(int_state)
    fpr = global_counter['FP'] / (global_counter['FP'] + global_counter['TN'])
    fpr_hist.append(fpr)
    timestamp_hist.append(int(time()))


if __name__ == "__main__":
    argv = sys.argv
    port_num = int(argv[1])
    output_filename = argv[2]

    unique_states = set()
    bit_array_len = 200
    hash_num = 6
    global_counter = Counter()
    a_values, b_values = hash_maker(hash_num, bit_array_len)
    global_bit_array = [False for _ in range(bit_array_len)]
    fpr_hist = []
    timestamp_hist = []

    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 10)
    lines = ssc.socketTextStream("localhost", port_num)
    state_stream = lines.transform(lambda rdd: rdd.map(json.loads).map(lambda x: x['state']))
    state_stream.foreachRDD(lambda rdd: process_stream(rdd))

    ssc.start()
    # try:
    #     ssc.awaitTermination()
    # except:
    ssc.awaitTermination(610)
    ssc.stop()

    with open(output_filename, 'w') as out_f:
        print("Time,FPR", file=out_f)
        for i in range(len(fpr_hist)):
            print("{0},{1}".format(datetime.fromtimestamp(timestamp_hist[i]), fpr_hist[i]), file=out_f)
