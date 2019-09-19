#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-09-14 01:44
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task3.py
# @Software: PyCharm

from pyspark import SparkContext
import sys

if __name__ == '__main__':
    argv = sys.argv
    review_file = argv[1]
    business_file = argv[2]
    output_file1 = argv[3]
    output_file2 = argv[4]
    print("Input1: ", review_file)
    print("Input2: ", business_file)
    print("Output1: ", output_file1)
    print("Output2: ", output_file2)