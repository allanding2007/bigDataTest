#!/usr/bin/env python
#encoding=utf-8
#
#File Name: fake_status.py
#Description: fake status data, and push to kafka.
#

import json
import random
import time
from kafka import KafkaProducer


class kfkProHandler(object):
    """
    """
    def __init__(self, config):
        self._config = config
        print("config is :{0}".format(config))
        self._init_kfk()

    def _init_kfk(self):
        self._producer = KafkaProducer(bootstrap_servers=self._config['hosts'])
        print("Kafka producer inited...")

    def produce(self, msg):
        self._producer.send(self._config['topic'], msg)
        print("Produce data: {0}".format(msg))


class fakeStatusHandler(kfkProHandler):
    """
    """
    def __init__(self, config):
        kfkProHandler.__init__(self, config)

    def _fake(self):
        data = self._generate_status()
        self.produce(data)

    @property
    def logger(self):
        return self._logger

    @logger.setter
    def logger(self, logger):
        self._logger = logger

    def _generate_status(self):
        client = "client_{0}"
        client_dict = map(lambda i: {
            "user": client.format(i),
            "tx": random.randint(50, 100),
            "rx": random.randint(0, 50)
        }, xrange(10))
        print(client_dict)
        return json.dumps(client_dict)

    def work(self):
        print("Begin to loop to fake status data.")

        while True:
            self._fake()
            time.sleep(0.5)


if __name__  == "__main__":
    config = {
        "hosts": "localhost:9092",
        "topic": "fake_status"
    }
    faker = fakeStatusHandler(config)
    faker.work()




