#!/usr/bin/python
from __future__ import print_function

import json
import sys
import time
from datetime import datetime

from pyspark import SparkContext
from common.adv_analytics.kafkaUtils import KafkaConnector
from util.utils import Utils

config = Utils.load_config(sys.argv[1])
sc = SparkContext(appName=config.property('spark.appName'), master=config.property('spark.master'))
sc.setLogLevel("WARN")
sc.addPyFile('/odh/python/common/adv_analytics/graphiteSink.py')
# log4jLogger = sc._jvm.org.apache.log4j
# LOGGER = log4jLogger.LogManager.getLogger(__name__)

from graphiteSink import CarbonSink


class StreamManipulate(object):
    """
    Process each batch from input kafka stream
    """
    def __init__(self, conf):
        """
        Obtain all options from config file and build kafka producer/consumer.
        :param conf: (dict) topic name
        """
        super(StreamManipulate, self).__init__()
        self.conf = conf
        self.metric_keys = ['virtualdisk', 'datastore', 'disk', 'cpu', 'summary', 'mem', 'net', ]

    def json_check(self, msg):
        try:
            msg = json.loads(msg[1])
        except (ValueError, TypeError) as e:
            return False
        if all(metric in msg for metric in self.metric_keys):
            return True
        else:
            return False

    def format_metrics(self, msg):
        res = []
        vm = msg['name']
        timestamp = int(time.mktime(datetime.strptime(msg['datetime'], "%Y-%m-%dT%H:%M:%SZ").timetuple()))
        carbon_path = self.conf.property('graphite.carbonPath')
        for metric_name in self.metric_keys:
            for i in msg[metric_name]:
                metric_path = "{}.{}.{}.{}".format(carbon_path, vm, metric_name, i)
                value = msg[metric_name][i]
                m = {}
                m['metric_path'] = metric_path
                m['value'] = value
                m['timestamp']= timestamp
                res.append(m)
        return res


    def build_out_stream(self, input_stream):
        """
        Each RDD/batch will be transformed so there is one element for each metric type per interval.
        :param input_stream:
        :return: aggregated stream
        """
        output = input_stream \
            .filter(self.json_check) \
            .map(lambda x: json.loads(x[1])) \
            .flatMap(lambda msg: self.format_metrics(msg))
        return output


if __name__ == "__main__":
    ssc = KafkaConnector.create_spark_context(config, sc)
    input_stream = KafkaConnector(config).create_kafka_stream(ssc)
    stream_build = StreamManipulate(config)
    output_stream = stream_build.build_out_stream(input_stream)
    # output_stream.pprint()
    sink = output_stream.foreachRDD(lambda rdd: rdd.foreachPartition(CarbonSink(config).send_partition))
    ssc.start()
    ssc.awaitTermination()
