#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 12/2/19 2:11 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_utils.py
# @Software: PyCharm
from pyspark import SparkContext
import csv


def cal_RMSE(output_file_path, test_file_path):
    # conf = SparkConf().set('spark.driver.host', '127.0.0.1')
    # sc = SparkContext(master='local', appName='myAppName', conf=conf)
    sc = SparkContext.getOrCreate()
    raws = []
    with open(output_file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raws.append([row[0] + ',' + row[1], float(row[2])])
    predict_val_pair = sc.parallelize(raws)

    raws = []
    with open(test_file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raws.append([row[0] + ',' + row[1], float(row[2])])
    ground_val_pair = sc.parallelize(raws)

    val_RMSE = (ground_val_pair.join(predict_val_pair).map(lambda x: (x[1][0] - x[1][1]) ** 2).mean()) ** 0.5
    sc.stop()
    return val_RMSE


def error_distribution(output_file_path, test_file_path):
    # conf = SparkConf().set('spark.driver.host', '127.0.0.1')
    # sc = SparkContext(master='local', appName='myAppName', conf=conf)
    sc = SparkContext.getOrCreate()
    raws = []
    with open(output_file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raws.append([row[0] + ',' + row[1], float(row[2])])
    predict_val_pair = sc.parallelize(raws)

    raws = []
    with open(test_file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                raws.append([row[0] + ',' + row[1], float(row[2])])
    ground_val_pair = sc.parallelize(raws)
    err_dict = ground_val_pair.join(predict_val_pair).map(lambda x: abs(x[1][0] - x[1][1])).map(lambda x: (int(x), 1)).reduceByKey(
        lambda a, b: a + b).collectAsMap()
    for i in range(5):
        if i != 4:
            if i in err_dict:
                print(">=%d and <%d: %d" % (i, i + 1, err_dict[i]))
            else:
                print(">=%d and <%d: %d" % (i, i + 1, 0))
        else:
            if i in err_dict:
                print(">=4: %d" % err_dict[i])
            else:
                print(">=4: 0")
    sc.stop()


def write_predict(test_pred_id_pairs, output_file_path):
    with open(output_file_path, 'w') as out_f:
        print("user_id, business_id, prediction", file=out_f)
        for line in test_pred_id_pairs:
            print(line, file=out_f)
