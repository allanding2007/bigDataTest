#!/usr/bin/env python
#coding=utf-8
#
#FileName: kfk_spark_streaming.py
#Created on: 07/09/2016, by allan


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
#import pyspark_cassandra
#from pyspark_cassandra import streaming
from datetime import datetime
import json
import redis

def insert_redis(rdd):
    print "collect rdd*********"
    redis_conn = redis.StrictRedis("127.0.0.1", 6379, 5)
    for i in rdd.collect():
        print "The data is:++++++++++"
        print i
        redis_conn.set("streaming", i, 100)
        print type(i)
        data = json.loads(i)
        print type(data)
        print data

sc = SparkContext(appName="spark_streaming")
ssc = StreamingContext(sc, 5)

topic = "fake_status"
kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming",{topic: 1})

raw = kafkaStream.map(lambda stream_data: stream_data)
time_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
clean = raw.map(lambda xs: xs[1])

#clean.pprint()
raw.pprint()
clean.pprint()
clean.foreachRDD(insert_redis)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to
#terminate
