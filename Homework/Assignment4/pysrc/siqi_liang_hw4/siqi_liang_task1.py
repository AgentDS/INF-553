#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/19/19 9:07 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task1.py
# @Software: PyCharm

import sys
import csv
from pyspark import SparkContext
from pyspark.sql import SQLContext
from graphframes import *


def emit_undirected_edge(directed_edges):
    for edge in directed_edges:
        yield (edge[0], edge[1])
        yield (edge[1], edge[0])


def vertex_combiner(vertexes):
    yield list(set(list(vertexes)))


if __name__ == "__main__":
    argv = sys.argv
    input_file = argv[1]
    community_output_file_path = argv[2]

    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    raw_data = sc.textFile(input_file, 4)

    edge_pairs = raw_data.mapPartitions(lambda x: csv.reader(x, delimiter=' '))
    edge_pairs.persist()
    vertexes = edge_pairs.flatMap(lambda x: x).mapPartitions(lambda vertexes: vertex_combiner(vertexes)).flatMap(
        lambda x: x).distinct().map(
        lambda x: (x,))
    vertexes.persist()
    undirected_edges = edge_pairs.mapPartitions(lambda directed_edges: emit_undirected_edge(directed_edges))

    vertexes_df = vertexes.toDF(['id'])
    edge_df = undirected_edges.toDF(['src', 'dst'])

    g = GraphFrame(vertexes_df, edge_df)
    label_communities = g.labelPropagation(maxIter=5).rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda a, b: a + b)
    sorted_communities = label_communities.map(lambda x: sorted(x[1])).sortBy(lambda x: [len(x), x[0]]).collect()
    with open(community_output_file_path, 'w') as out_f:
        for com in sorted_communities:
            new_com = ["'%s'" % i for i in com]
            line = ', '.join(new_com)
            print(line, file=out_f)
