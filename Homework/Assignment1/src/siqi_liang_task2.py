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

if __name__ == '__main__':
    argv = sys.argv
    user_file = argv[1]
    output_file = argv[2]
    n_partition = int(argv[3])
    print("Input: ", user_file)
    print("Output: ", output_file)
    print("n_partition: ", n_partition)

    task2_json = {
        "default": {"n_partition": default_n_partition, "n_items": default_n_items, "exe_time": defualt_exe_time},
        "customized": {"n_partition": customized_n_partition, "n_items": customized_n_items,
                       "exe_time": customized_exe_time},
        "explanation": explanation
    }
    with open(output_file, "w") as write_file:
        json.dump(task2_json, write_file)
