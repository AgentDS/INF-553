#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-09-18 23:41
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : RDD_ex.py
# @Software: PyCharm

from pyspark import SparkContext

if __name__ == "__main__":
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
