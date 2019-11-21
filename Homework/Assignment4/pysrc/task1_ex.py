#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/20/19 4:58 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : task1_ex.py
# @Software: PyCharm

import sys
import csv
from pyspark import SparkContext
from pyspark.sql import SQLContext
from graphframes import *
from time import time


def vertex_combiner(vertexes):
    yield list(set(list(vertexes)))


def run_all(minPartition):
    input_file = "/Users/liangsiqi/Desktop/INF-553/Homework/Assignment4/data/power_input.txt"
    community_output_file_path = "./local_task1.txt"
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    raw_data = sc.textFile(input_file, minPartition)

    edge_pairs = raw_data.mapPartitions(lambda x: csv.reader(x, delimiter=' '))
    edge_pairs.persist()
    vertexes = edge_pairs.flatMap(lambda x: x).mapPartitions(lambda vertexes: vertex_combiner(vertexes)).flatMap(
        lambda x: x).distinct().map(
        lambda x: (x,))
    vertexes.persist()

    vertexes_df = vertexes.toDF(['id'])
    edge_df = edge_pairs.toDF(['src', 'dst'])

    g = GraphFrame(vertexes_df, edge_df)
    label_communities = g.labelPropagation(maxIter=5).rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda a, b: a + b)
    sorted_communities = label_communities.map(lambda x: sorted(x[1])).sortBy(lambda x: [len(x), x[0]]).collect()
    sc.stop()
    with open(community_output_file_path, 'w') as out_f:
        for com in sorted_communities:
            line = ', '.join(com)
            print(line, file=out_f)


if __name__ == "__main__":
    for i in [3, 4, 5, 6, 7]:
        start = time()
        run_all(i)
        end = time()
        duration = end - start
        print("partition=%d, duration=%d" % (i, duration))
