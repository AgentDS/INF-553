#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/26/19 2:29 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : dstream_ex.py
# @Software: PyCharm

from pyspark import SparkContext
import json
from pyspark.streaming import StreamingContext




if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("/Users/liangsiqi/Documents/checkpoint")
    line = ssc.socketTextStream("localhost", 9999)
    words = line.map(lambda x: x.split(' '))
    # wordOne = words.map(lambda x: x+"_one")
    # wordTwo = words.map(lambda x: x + "_two")
    # unionWords = wordOne.union(wordTwo)
    # wordsCount = words.count()
    # reduceWords = words.reduce(lambda a, b: a + '-' + b)
    # countByValueWords = words.countByValue()
    # pairs = words.map(lambda x: (x, 1))
    # wordCounts = pairs.reduceByKey(lambda a, b: a + b)
    # wordsOne = words.map(lambda x: (x, x+'_one'))
    # wordsTwo = words.map(lambda x: (x, x + '_two'))
    # joinWords = wordsOne.join(wordsTwo)
    # words = line.transform(lambda rdd: rdd.flatMap(lambda x: x.split(' ')))
    # windowWords = words.countByWindow(3, 1)

    words.pprint()

    # unionWords.pprint()
    # wordOne.pprint()
    # wordTwo.pprint()
    # wordsCount.pprint()
    # reduceWords.pprint()
    # countByValueWords.pprint()
    # wordCounts.pprint()
    # wordsOne.pprint()
    # wordsTwo.pprint()
    # joinWords.pprint()
    # windowWords.pprint()
    ssc.start()
    ssc.awaitTermination()
