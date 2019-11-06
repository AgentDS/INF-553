#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/5/19 3:31 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : task1_local_experiment.py
# @Software: PyCharm

from pyspark import SparkContext
import sys
import csv
import random
from time import time
from siqi_liang_task1 import *

if __name__ == "__main__":
    input_file = "/Users/liangsiqi/Documents/Dataset/yelp_rec_data/yelp_train.csv"
    output_file = "./local_t1.csv"
    log_path = "./experiment_time.txt"
    tf_pair_cnts = []
    par_pair_cnts = []
    tf_precition_hist = []
    par_precition_hist = []
    tf_recall_hist = []
    par_recall_hist = []
    tf_duration_hist = []
    par_duration_hist = []
    numPartitions_hist = []

    ground_truth = get_ground_truth()

    for numPartitions in range(2, 16):
        print("numPartitions: %d======" % numPartitions)
        duration_tf, res_lines_tf = using_textFile(input_file, output_file, numPartitions)
        res_tf = get_res_str_pairs(res_lines_tf)

        duration_par, res_lines_par = using_parallelize(input_file, output_file, numPartitions)
        res_par = get_res_str_pairs(res_lines_par)

        p_tf, r_tf = cal_precision_recall(res_tf, ground_truth)
        p_par, r_par = cal_precision_recall(res_par, ground_truth)

        tf_pair_cnts.append(len(res_lines_tf))
        par_pair_cnts.append(len(res_lines_par))
        tf_precition_hist.append(p_tf)
        par_precition_hist.append(p_par)
        tf_recall_hist.append(r_tf)
        par_recall_hist.append(r_par)
        tf_duration_hist.append(duration_tf)
        par_duration_hist.append(duration_par)

        numPartitions_hist.append(numPartitions)

    with open(log_path, 'w') as log_file:
        print("                  numPartitions       found_pair_nums       precision       recall       time(s)",
              file=log_file)
        for i in range(len(numPartitions_hist)):
            print("textFile              %2d                   %3d               %.2f           %.2f         %4d" % (
                numPartitions_hist[i], tf_pair_cnts[i], tf_precition_hist[i], tf_recall_hist[i], tf_duration_hist[i]), file=log_file)

        for i in range(len(numPartitions_hist)):
            print("parallelize           %2d                   %3d               %.2f           %.2f         %4d" % (
                numPartitions_hist[i], par_pair_cnts[i], par_precition_hist[i], par_recall_hist[i], par_duration_hist[i]), file=log_file)
