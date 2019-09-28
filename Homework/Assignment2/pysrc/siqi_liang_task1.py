#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/27/19 3:02 AM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task1.py
# @Software: PyCharm

from pyspark import SparkContext
import json
import sys

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
    clean_data = raw_data_without_header.map(lambda line: [int(i) for i in line.strip().split(',')])

    if case_number == 1:
        baskets = clean_data.map(lambda x: [x[0], [x[1]]]).reduceByKey(lambda a, b: a + b).map(lambda x: [x[0], list(set(list(x[1])))])
    else:
        baskets = clean_data.map(lambda x: [x[1], [x[0]]]).reduceByKey(lambda a, b: a + b).map(lambda x: [x[0], list(set(list(x[1])))])
    baskets.persist()
    print(baskets.collect())

# print("Case number: ", case_number)
# print("Support: ", support)
# print("Input file: ", input_file)
