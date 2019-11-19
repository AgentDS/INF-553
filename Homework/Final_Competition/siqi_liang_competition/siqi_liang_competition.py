#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/18/19 9:31 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_competition.py
# @Software: PyCharm
import siqi_liang_HierarchicalWeight as HWeight
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    argv = sys.argv
    input_path = argv[1]
    test_file_name = argv[2]
    result_file_name = argv[3]

    train_file_name = "yelp_train.csv"
    HWeight.global_AVG_method(input_path + train_file_name, test_file_name, result_file_name, 4)
