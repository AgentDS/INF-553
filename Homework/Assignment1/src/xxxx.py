#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 9/22/19 3:20 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : xxxx.py
# @Software: PyCharm
from pyspark import SparkContext
import sys
import json
import time

if __name__ == '__main__':
    start = time.time()
    argv = sys.argv
    user_file = argv[1]
    output_file = argv[2]

    sc = SparkContext()
    userRDD = sc.textFile(user_file)
    userRDD_json = userRDD.map(json.loads)

    # A. total number of users
    user_cnt = userRDD_json.count()

    # B. average number of reviews
    review_cnt_RDD = userRDD_json.map(lambda x: [x['user_id'], x['review_count']])
    review_cnt_RDD.persist()
    review_cnt_mean = review_cnt_RDD.values().mean()

    # C. number of distinct user names
    user_nameRDD = userRDD_json.map(lambda x: x['name']).groupBy(lambda x: x)
    user_nameRDD.persist()  # saved in memory after the first time it is computed
    user_name_cnt = user_nameRDD.count()

    # D. number of users joined in 2011
    user_2011_cnt = userRDD_json.filter(lambda x: x['yelping_since'][:4] == '2011').count()

    # E. Top 10 popular names and the number of times they
    #    appear (user names that appear the most number of times)
    user_name_cntRDD = user_nameRDD.map(lambda x: [x[0], len(x[1])])
    most_name = user_name_cntRDD.sortBy(lambda x: x[1], ascending=False).take(10)

    # F. Find Top 10 user ids who have written the most number of reviews (0.5 point)
    most_review = review_cnt_RDD.sortBy(lambda x: x[1], ascending=False).take(10)

    # output
    task1_json = {'total_users': user_cnt,
                  'avg_reviews': review_cnt_mean,
                  'distinct_usernames': user_name_cnt,
                  'num_users': user_2011_cnt,
                  'top10_popular_names': most_name,
                  'top10_most_reviews': most_review}

    with open(output_file, "w") as write_file:
        json.dump(task1_json, write_file)

    print("Time: %.4f s" % (time.time() - start))
