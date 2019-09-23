#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-09-18 23:41
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : RDD_ex.py
# @Software: PyCharm

from pyspark import SparkContext
import json




def RDD_basic():
    sc = SparkContext()
    intRDD = sc.parallelize([3, 1, 2, 5, 5])
    stringRDD = sc.parallelize(['Apple', 'Orange', 'Grape', 'Banana', 'Apple'])
    print(intRDD.collect())
    print(stringRDD.collect())
    print(intRDD.map(lambda x: x + 1).collect())
    print(intRDD.filter(lambda x: x < 3).collect())
    print(stringRDD.filter(lambda x: 'ra' in x).collect())
    print(intRDD.distinct().collect())
    sRDD = intRDD.randomSplit([0.4, 0.6])
    print(len(sRDD))
    print(sRDD[0].collect())
    print(sRDD[1].collect())

    print("Group by:")
    result = intRDD.groupBy(lambda x: x % 2).collect()
    print(sorted([(x, sorted(y)) for (x, y) in result]))

    print("\n\nMultiple RDD:")
    intRDD1 = sc.parallelize([3, 1, 2, 5, 5])
    intRDD2 = sc.parallelize([5, 6])
    intRDD3 = sc.parallelize([2, 7])
    print(intRDD1.union(intRDD2).union(intRDD3).collect())
    print(intRDD1.intersection(intRDD2).collect())
    print(intRDD1.cartesian(intRDD2).collect())

    print("\n\nBasic action:")
    print(intRDD.first())
    print(intRDD.take(2))
    print(intRDD.takeOrdered(3))
    print(intRDD.takeOrdered(3, lambda x: -x))

    print("\n\nBasic key-Value transform operation:")
    kvRDD1 = sc.parallelize([(3, 4), (3, 6), (5, 6), (1, 2)])
    print(kvRDD1.keys().collect())
    print(kvRDD1.values().collect())
    print("Filter using key: ")
    print(kvRDD1.filter(lambda x: x[0] < 5).collect())
    print("Filter using values: ")
    print(kvRDD1.filter(lambda x: x[1] < 5).collect())
    print("MapValues:")
    print(kvRDD1.mapValues(lambda x: x ** 2).collect())
    print("Sort by key:")
    print(kvRDD1.sortByKey().collect())
    print(kvRDD1.sortByKey(ascending=True).collect())
    print(kvRDD1.sortByKey(ascending=False).collect())

    print("\n\nMultiple key-value:")
    kvRDD1 = sc.parallelize([(3, 4), (3, 6), (5, 6), (1, 2)])
    kvRDD2 = sc.parallelize([(3, 8)])
    print(kvRDD1.join(kvRDD2).collect())
    print(kvRDD1.leftOuterJoin(kvRDD2).collect())
    print(kvRDD1.rightOuterJoin(kvRDD2).collect())
    print(kvRDD1.subtractByKey(kvRDD2).collect())

    print("\n\nKey-Value actions:")
    print(kvRDD1.first())
    print(kvRDD1.take(2))
    print(kvRDD1.first()[0])
    print(kvRDD1.first()[1])
    print("Count by key:")
    print(kvRDD1.countByKey())
    print(kvRDD1.lookup(3))

    print("\n\nPersistent RDD:")
    kvRDD1.persist()
    kvRDD1.unpersist()

if __name__ == "__main__":
    # path = "/Users/liangsiqi/Documents/Dataset/yelp_dataset/"
    # user_file = "user.json"
    # # users = [json.loads(line) for line in open(path + user_file, 'r')]
    # # print(users[0])
    # sc = SparkContext()
    # userRDD = sc.textFile(path + user_file)
    # print(userRDD.first())
    RDD_basic()