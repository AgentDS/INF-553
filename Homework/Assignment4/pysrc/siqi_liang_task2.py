#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/20/19 6:45 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task2.py
# @Software: PyCharm

import csv
from pyspark import SparkContext
import copy
from itertools import combinations
import sys


def vertex_combiner(vertexes):
    yield list(set(list(vertexes)))


def BFS_travel(root_node, vert_neighbors, vert_list):
    vert_neighbors = copy.deepcopy(vert_neighbors)
    vert_list = copy.deepcopy(vert_list)
    in_degree = {i: [] for i in vert_list}  # {n1:[from_node1, from_node2, ...], n2:[from_node1, ...], ...}
    out_degree = {i: [] for i in vert_list}  # {n1:[to_node1, to_node2, ...], n2:[to_node1, to_node2, ...], ...}

    current_level = [root_node]

    while True:
        next_level = []
        for current_node in current_level:
            vert_list.remove(current_node)
            for node in vert_neighbors:
                if current_node in vert_neighbors[node]:
                    vert_neighbors[node].remove(current_node)

        for current_node in current_level:
            current_child = vert_neighbors[current_node]
            next_level.extend(current_child)

            for node in current_child:
                in_degree[node].append(current_node)
                out_degree[current_node].append(node)
        current_level = list(set(next_level))
        if len(current_level) == 0:
            break
    return in_degree, out_degree


def tree_edge_value(vert_list, in_degree, out_degree):
    in_degree = copy.deepcopy(in_degree)
    in_degree_num = {i: len(in_degree[i]) for i in in_degree}
    out_degree = copy.deepcopy(out_degree)
    vert_list = copy.deepcopy(vert_list)

    edges = {}
    node_values = {i: 0 for i in vert_list}
    empty_out = []

    # initialize
    for node in out_degree:
        if len(out_degree[node]) == 0:
            empty_out.append(node)

    while len(empty_out) > 0:
        for node in empty_out:
            if in_degree_num[node] > 0:
                node_values[node] = (node_values[node] + 1) / in_degree_num[node]
            else:
                node_values[node] += 1
            for src_node in in_degree[node]:
                out_degree[src_node].remove(node)
                node_values[src_node] += node_values[node]
            out_degree.pop(node)
        empty_out = []
        for node in out_degree:
            if len(out_degree[node]) == 0:
                empty_out.append(node)

    for node in node_values:
        for src_node in in_degree[node]:
            edge_name = '-'.join(sorted([src_node, node]))
            edges[edge_name] = node_values[node]
    return edges


def emit_edges_for_each_node(node_subset, vert_neighbors, vert_list):
    for root_node in node_subset:
        in_degree, out_degree = BFS_travel(root_node, vert_neighbors, vert_list)
        edge_values = tree_edge_value(vert_list, in_degree, out_degree)
        for edge in edge_values:
            yield (edge, edge_values[edge])


def emit_undirected_edge(directed_edges):
    for edge in directed_edges:
        yield (edge[0], [edge[1]])
        yield (edge[1], [edge[0]])


def emit_node_pair_betweenness(edges):
    for edge in edges:
        nodes = edge[0].split('-')
        yield (nodes, edge[1])


def write_into_file(out_file, edge_info):
    with open(out_file, 'w') as out_f:
        for edge in edge_info:
            print("('{0}', '{1}'), {2:f}".format(edge[0][0], edge[0][1], edge[1]), file=out_f)


def assign_community_to_nodes(vert_neighbors, vert_list):
    vertex_list = copy.deepcopy(vert_list)
    vert_neighbors = copy.deepcopy(vert_neighbors)
    node_community = {}
    community_idx = 0

    while len(vertex_list) > 0:
        current_community = []
        root_node = vertex_list[0]
        current_level = [root_node]
        while True:
            next_level = []
            for current_node in current_level:
                if current_node in vertex_list:
                    vertex_list.remove(current_node)
                    current_community.append(current_node)
                current_child = vert_neighbors[current_node]
                for node in current_child:
                    if node in vertex_list:
                        next_level.append(node)

            current_level = list(set(next_level))
            if len(current_level) == 0:
                node_community[community_idx] = list(set(current_community))
                community_idx += 1
                break
    return node_community


def graph_modularity(node_community, m, original_vert_neighbors_num, current_vert_neighbors):
    # acc_sum actually is not the exact value of modularoty,
    # but same as it when only used for comparision
    acc_sum = 0
    for label in node_community:
        community = node_community[label]
        for pair in combinations(community, 2):
            i, j = pair
            if j in current_vert_neighbors[i]:
                Aij = 1
            else:
                Aij = 0
            ki = original_vert_neighbors_num[i]
            kj = original_vert_neighbors_num[j]
            acc_sum += Aij - ki * kj / (2 * m)
    return acc_sum


if __name__ == "__main__":
    argv = sys.argv
    input_file = argv[1]
    betweenness_output_file_path = argv[2]
    community_output_file_path = argv[3]

    sc = SparkContext.getOrCreate()
    raw_data = sc.textFile(input_file, 5)
    edge_pairs = raw_data.mapPartitions(lambda x: csv.reader(x, delimiter=' '))
    edge_pairs.persist()
    vertexes = edge_pairs.flatMap(lambda x: x).mapPartitions(lambda vertexes: vertex_combiner(vertexes)).flatMap(
        lambda x: x).distinct().map(lambda x: x)
    vertexes.persist()
    vertex_neighbors = edge_pairs.mapPartitions(lambda edges: emit_undirected_edge(edges)).reduceByKey(lambda a, b: a + b).map(
        lambda x: (x[0], sorted(x[1]))).collectAsMap()
    all_vertexes = vertexes.collect()
    edge_info = vertexes.mapPartitions(
        lambda node_subset: emit_edges_for_each_node(node_subset, vertex_neighbors, all_vertexes)).reduceByKey(
        lambda a, b: a + b).mapValues(lambda x: x / 2).mapPartitions(lambda edges: emit_node_pair_betweenness(edges)).sortBy(
        lambda x: [-x[1], x[0][0]]).collect()
    write_into_file(betweenness_output_file_path, edge_info)
    m = len(edge_info)
    original_vertex_neighbors = copy.deepcopy(vertex_neighbors)
    original_vertex_neighbors_num = {i: len(original_vertex_neighbors[i]) for i in original_vertex_neighbors}
    old_community_num = 1
    old_modularity = 0  # the original modularity is 1.77097130

    while True:
        # print("Current community number:", old_community_num)
        if old_community_num == 18:
            break
        while True:
            # take the edge with the maximum betweenness
            node1, node2 = vertexes.mapPartitions(
                lambda node_subset: emit_edges_for_each_node(node_subset, vertex_neighbors, all_vertexes)).reduceByKey(
                lambda a, b: a + b).mapPartitions(lambda edges: emit_node_pair_betweenness(edges)).sortBy(
                lambda x: [-x[1], x[0][0]]).take(1)[0][0]
            # remove edge from neighbor list
            vertex_neighbors[node1].remove(node2)
            vertex_neighbors[node2].remove(node1)
            node_community = assign_community_to_nodes(vertex_neighbors, all_vertexes)
            # calculate modularity only if number of communities changes
            if len(node_community) > old_community_num:
                break
        old_community_num = len(node_community)
        modularity = graph_modularity(node_community, m, original_vertex_neighbors_num, vertex_neighbors)

    final_communities = [sorted(i) for i in node_community.values()]
    sorted_communities = sorted(final_communities, key=lambda x: [len(x), x[0]])
    with open(community_output_file_path, 'w') as out_f:
        for com in sorted_communities:
            new_com = ["'%s'" % i for i in com]
            line = ', '.join(new_com)
            print(line, file=out_f)
