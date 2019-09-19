#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-09-14 01:43
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task1.py
# @Software: PyCharm

import pyspark as ps
import sys

if __name__ == '__main__':
    argv = sys.argv
    user_file = argv[1]
    output_file = argv[2]
    print("Input: ", user_file)
    print("Output: ", output_file)
