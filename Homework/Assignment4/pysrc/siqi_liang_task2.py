#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/20/19 6:45 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task2.py
# @Software: PyCharm

import csv
from pyspark import SparkContext


def vertex_combiner(vertexes):
    yield list(set(list(vertexes)))


def emit_undirected_edge(directed_edges):
    for edge in directed_edges:
        yield (edge[0], [edge[1]])
        yield (edge[1], [edge[0]])


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    input_file = "/Users/liangsiqi/Desktop/INF-553/Homework/Assignment4/data/power_input.txt"
    raw_data = sc.textFile(input_file)
    edge_pairs = raw_data.mapPartitions(lambda x: csv.reader(x, delimiter=' '))
    vertexes = edge_pairs.flatMap(lambda x: x).mapPartitions(lambda vertexes: vertex_combiner(vertexes)).flatMap(
        lambda x: x).distinct().map(lambda x: x)
    vertex_neighbors = edge_pairs.mapPartitions(lambda edges: emit_undirected_edge(edges)).reduceByKey(lambda a, b: a + b).map(
        lambda x: (x[0], sorted(x[1]))).collectAsMap()
    all_vertexes = vertexes.collect()
