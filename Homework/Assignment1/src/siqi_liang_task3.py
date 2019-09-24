#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-09-14 01:44
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task3.py
# @Software: PyCharm

from pyspark import SparkContext
import sys
import json
from time import time

if __name__ == '__main__':
    argv = sys.argv
    review_file = argv[1]
    business_file = argv[2]
    output_fileA = argv[3]
    output_fileB = argv[4]

    sc = SparkContext()
    reviewRDD = sc.textFile(review_file).map(json.loads)
    businessRDD = sc.textFile(business_file).map(json.loads)

    business_state_kvRDD = businessRDD.map(lambda x: [x['business_id'], x['state']])
    business_id_star_kvRDD = reviewRDD.map(lambda x: [x['business_id'], x['stars']])
    business_id_star_combine = business_id_star_kvRDD.combineByKey((lambda x: (x, 1)),  # ceateCombiner
                                                                   (lambda x, value: (x[0] + value, x[1] + 1)),  # mergeValue
                                                                   (lambda x, y: (x[0] + y[0], x[1] + y[1])))

    interm_state_stars_cnt_combine = business_id_star_combine.join(business_state_kvRDD).map(lambda x: (x[1][1], x[1][0]))

    interm_state_stars_cnt = interm_state_stars_cnt_combine.combineByKey((lambda x: (x, 1)),  # ceateCombiner
                                                                         (lambda c, value: ((c[0][0] + value[0], c[0][1] + value[1]),
                                                                                            c[1] + 1)),  # mergeValue
                                                                         (lambda x, y: ((x[0][0] + y[0][0], x[0][1] + y[0][1]),
                                                                                        x[1] + y[1])))
    avg_state = interm_state_stars_cnt.map(lambda x: [x[0], x[1][0][0] / x[1][0][1]]).sortBy(lambda x: (-x[1], x[0]))
    start1 = time()
    resultA = avg_state.collect()
    for i in range(5):
        print(resultA[i][0])
    end1 = time()
    m1 = end1 - start1

    start2 = time()
    resultB = avg_state.take(5)
    for i in range(5):
        print(resultB[i][0])
    end2 = time()
    m2 = end2 - start2

    sc.stop()
    with open(output_fileA, "w") as outA:
        outA.write("state,stars\n")
        for iterm in resultA:
            outA.write("%s,%.3f\n" % (iterm[0], iterm[1]))

    explanation = "collect() will copy the whole dataset to the driver, which takes longer time than take(). take() retrieve only a capped number of elements instead"

    B_json = {'m1': m1, 'm2': m2, 'explanation': explanation}
    with open(output_fileB, "w") as write_file:
        json.dump(B_json, write_file)
