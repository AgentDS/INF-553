#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/25/19 1:25 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task2.py
# @Software: PyCharm

import sys

if __name__ == "__main__":
    argv = sys.argv
    train_file_path = argv[1]
    test_file_path = argv[2]
    case_id = int(argv[3])
    output_file_path = argv[4]

    if case_id == 1:
        # Model-based CF
        pass
    elif case_id == 2:
        # User-based CF
        pass
    elif case_id == 3:
        # Item-based CF
        pass
    else:
        pass
