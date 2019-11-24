#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/23/19 1:18 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : xxx.py
# @Software: PyCharm

import copy


def BFS_travel(root_node, vert_neighbors, vert_list):
    vert_neighbors = copy.deepcopy(vert_neighbors)
    vert_list = copy.deepcopy(vert_list)
    in_degree = {i: [] for i in vert_list}  # {n1:[from_node1, from_node2, ...], n2:[from_node1, ...], ...}
    out_degree = {i: [] for i in vert_list}  # {n1:[to_node1, to_node2, ...], n2:[to_node1, to_node2, ...], ...}
    depth_table = {i: None for i in vert_list}  # {n1: depth1, n2: depth2, ...}
    current_depth = 1

    current_level = [root_node]

    while len(vert_list) > 0:
        next_level = []
        for current_node in current_level:
            vert_list.remove(current_node)
            for node in vert_neighbors:
                if current_node in vert_neighbors[node]:
                    vert_neighbors[node].remove(current_node)

        for current_node in current_level:
            depth_table[current_node] = current_depth
            current_child = vert_neighbors[current_node]
            next_level.extend(current_child)

            for node in current_child:
                in_degree[node].append(current_node)
                out_degree[current_node].append(node)
        current_level = list(set(next_level))
        current_depth += 1
    return in_degree, out_degree, depth_table


def edge_value_calculate(vert_list, in_degree, out_degree, depth_table):
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


if __name__ == "__main__":
    vertex_neighbors = {'A': ['B', 'C'],
                        'B': ['A', 'C', 'D'],
                        'C': ['A', 'B'],
                        'D': ['B', 'E', 'F', 'G'],
                        'E': ['D', 'F'],
                        'F': ['D', 'E', 'G'],
                        'G': ['D', 'F']}
    all_vertexes = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    in_degree, out_degree, depth_table = BFS_travel('E', vertex_neighbors, all_vertexes)
    print("In degree: ", in_degree)
    print("Out degree: ", out_degree)

    edges = edge_value_calculate(all_vertexes, in_degree, out_degree, depth_table)
    print("Edge values: ", edges)
