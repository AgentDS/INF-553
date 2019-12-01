#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 11/25/19 10:37 PM
# @Author  : Siqi Liang
# @Contact : zszxlsq@gmail.com
# @File    : siqi_liang_task3.py
# @Software: PyCharm

import sys
# from pyspark import SparkContext
import json
import random
from collections import Counter
# from pyspark.streaming import StreamingContext
from time import time
from datetime import datetime
import tweepy
import re


def all_tags(text):
    word_pool = re.split("\s+|[,;.-/:’'—?Ⓝ]\s*", text)
    word_pool = [i.rstrip('…').rstrip('!') for i in word_pool]
    tag_list = []
    for word in word_pool:
        if len(word) >= 2:
            if word[0] == '#':
                n = len(word)
                flag = True
                for i in range(1, n):
                    if word[i] == '#':
                        flag = False
                        break
                if flag is True:
                    if word[1].isdigit():
                        if len(word[1:]) > 2:
                            for i in word[1:]:
                                if not i.isdigit() and i != '_':
                                    tag_list.append(word.strip('#'))
                                    break
                    else:
                        tag_list.append(word.strip('#'))
    return tag_list


class MyStreamListener(tweepy.StreamListener):
    tag_hist = {}
    sequence_num = 0
    total_cnt = 0
    tweet_buffer = []

    def on_status(self, status):
        # print(status.text)
        # print("|||||||||||||||||||||||")
        tags_to_add = all_tags(status.text)
        if len(tags_to_add) > 0:
            MyStreamListener.total_cnt += 1
            if MyStreamListener.sequence_num == 150:
                prob_to_add = random.random()
                if prob_to_add <= 150 / MyStreamListener.total_cnt:
                    idx_to_abort = int(random.random() * 150)
                    text_to_abort = MyStreamListener.tweet_buffer.pop(idx_to_abort)
                    tags_to_abort = all_tags(text_to_abort)
                    self._delete_tage(tags_to_abort)

                    MyStreamListener.tweet_buffer.append(status.text)
                    self._add_tags(tags_to_add)

            elif MyStreamListener.sequence_num < 150:
                tags_to_add = all_tags(status.text)
                if len(tags_to_add) > 0:
                    MyStreamListener.sequence_num += 1
                    MyStreamListener.tweet_buffer.append(status.text)
                    self._add_tags(tags_to_add)

            print("The number of tweets with tags from the beginning: ", MyStreamListener.total_cnt)
            self._sort_tag_hist_print()

    def _delete_tage(self, tags_to_adopt):
        for tag in tags_to_adopt:
            MyStreamListener.tag_hist[tag] -= 1
            if MyStreamListener.tag_hist[tag] == 0:
                MyStreamListener.tag_hist.pop(tag)

    def _add_tags(self, tags_to_delete):
        for tag in tags_to_delete:
            if tag not in MyStreamListener.tag_hist:
                MyStreamListener.tag_hist[tag] = 0
            MyStreamListener.tag_hist[tag] += 1

    def _sort_tag_hist_print(self):
        top_tags = sorted(list(MyStreamListener.tag_hist.items()), key=lambda x: [-x[1], x[0]])[:5]
        for tag in top_tags:
            print("{0} : {1}".format(tag[0], tag[1]))
        print('')


if __name__ == "__main__":
    argv = sys.argv
    # port_num = int(argv[1])
    # output_filename = argv[2]

    consumer_key = 'IAlRSjkeynP1LFzgE5XGDjoZ2'
    consumer_secret = 'ny6LDWzeWh3nAzg5G0gVYDCSYXr43S9o13fC6YJulrYqJS4w8z'
    access_token = '974302581383086081-BTfVnhhQmUhVLRcJZ4oPjQ8tz2QZVVX'
    access_token_secret = '33wJTQI7tpANJsqJdu22Ba0CJw5kyBLVTGbgwbHdoG4xi'
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(track=['#'])

    # sc = SparkContext.getOrCreate()
    # ssc = StreamingContext(sc, 10)
    # lines = ssc.socketTextStream("localhost", 9999)
    # state_stream = lines.transform(lambda rdd: rdd.map(json.loads).map(lambda x: x['state']))
    # state_stream.foreachRDD(lambda rdd: process_stream(rdd))
