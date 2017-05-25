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


sc = SparkContext(appName="spark_streaming")
ssc = StreamingContext(sc, 5)

topic = "fake_status"
kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming",{topic: 1})
#kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"bootstrap.servers":"localhost:9092"})

raw = kafkaStream.flatMap(lambda kafkaS: [kafkaS])
time_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
clean = raw.map(lambda xs: xs[1].split(","))

#my_row = clean.map(lambda x: {
#          "testid": "test",
#          "time1": x[1],
#          "time2": time_now,
#          "delta": (datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S.%f') -\
#                           datetime.strptime(time_now, '%Y-%m-%d\
#                                             %H:%M:%S.%f')).microseconds,
#})

#my_row.pprint()
clean.pprint()

print "The result is:"
print "----------------------------------"
#print my_row
print clean

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to
terminate
