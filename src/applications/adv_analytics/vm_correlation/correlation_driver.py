#!/usr/bin/python
from __future__ import print_function

import json
import sys

from pyspark import SparkContext
from common.adv_analytics.kafkaUtils import KafkaConnector
from util.utils import Utils

config = Utils.load_config(sys.argv[1])
sc = SparkContext(appName=config.property('spark.appName'), master=config.property('spark.master'))
sc.setLogLevel("WARN")
sc.addPyFile('/odh/python/common/adv_analytics/lib/kazoo.zip')
sc.addPyFile('/odh/python/applications/adv_analytics/vm_correlation/add_vm_metrics.py')
sc.addPyFile('/odh/python/applications/adv_analytics/vm_correlation/kafkaSink.py')
# log4jLogger = sc._jvm.org.apache.log4j
# LOGGER = log4jLogger.LogManager.getLogger(__name__)

from add_vm_metrics import IngestVMData
from kafkaSink import KafkaOutput
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError


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
        self.metric_types = []

    def json_check(self, msg):
        try:
            msg = json.loads(msg[1])
        except (ValueError, TypeError) as e:
            return False
        if ("metric_name" in msg) and ("value" in msg):
            return True
        else:
            return False

    def create_pair(self, msg):
        msg["count"] = 1
        pair = (msg["metric_name"],msg)
        return pair

    def aggregate(self, msgA, msgB):
        agg = self.conf.property('correlate.aggregate')
        metric_type = msgA["metric_name"].split('.')[-1]
        count = msgA["count"] + msgB["count"]
        res = dict(msgB)
        res["count"] = count
        if metric_type == "count":
            res["value"] = msgA["value"] + msgB["value"]
        else:
            if agg == 'max':
                value = max([msgA["value"], msgB["value"]])
            elif agg == 'avg' or agg == 'sum':
                value = msgA["value"] + msgB["value"]
            else:
                raise ValueError('Invalid aggregate method (max/avg/sum)')
            res["value"] = value
        return res

    def apply_agg(self, msg):
        agg = self.conf.property('correlate.aggregate')
        val = dict(msg[1])
        if msg[0].split('.')[-1] != "count" and agg == 'avg':
            val["value"] = float(val["value"])/int(val["count"])
            return (val["hostname"], val)
        else:
            return (val["hostname"], val)

    def create_agg_stream(self, input_stream):
        """
        Each RDD/batch will be transformed so there is one element for each metric type per interval.
        :param input_stream:
        :return: aggregated stream
        """
        output = input_stream \
            .filter(self.json_check) \
            .map(lambda x: json.loads(x[1])) \
            .map(lambda msg: self.create_pair(msg)) \
            .reduceByKey(self.aggregate) \
            .map(lambda row: self.apply_agg(row)) \
            .groupByKey()
        return output

    def include_offsets(self, iter, zk_host, LOGGER=False):
        """
        Process each partition in RDD
        :param iter:
        :param zk_host:
        :param LOGGER:
        :return: stream partition
        """
        res = []
        zk = KazooClient(hosts=zk_host)
        zk.start()
        for record in iter:
            vm = record[0]
            try:
                zk_data = zk.get('/observer/vm/{}'.format(vm))
                zk_res = json.loads(zk_data[0])
            except NoNodeError:
                if LOGGER:
                    LOGGER.warn("failed to fetch partition/offset from zookeeper")
                zk_res = {}
            res.append((vm, zk_res))
        zk.stop()
        return res

    def build_output_stream(self, input_stream, config, LOGGER=False):
        """
        Get topic message holding the right vm metrics using partition and offset.
        :param input_stream: Input RDD DStream
        :param config: configuration dict
        :return: output_stream
        """
        bootstrap_servers = config.property('kafka.bootstrapServers')
        zk_host = config.property('kafka.zookeeperHosts').split('/kafka')[0]
        vm_topic = config.property('correlate.vmTopicName')

        output_stream = input_stream\
            .mapPartitions(lambda iter: self.include_offsets(iter, zk_host, LOGGER))\
            .map(lambda x: IngestVMData.include_vm(x, bootstrap_servers, vm_topic, LOGGER))

        return output_stream


if __name__ == "__main__":
    ssc = KafkaConnector.create_spark_context(config, sc)
    input_stream = KafkaConnector(config).create_kafka_stream(ssc)
    stream_build = StreamManipulate(config)
    agg_stream = stream_build.create_agg_stream(input_stream)
    agg_stream.cache()
    output_stream = stream_build.build_output_stream(agg_stream, config)
    joined = agg_stream.join(output_stream).flatMap(lambda x: IngestVMData.flatten(x)).filter(IngestVMData.filter_correlated)
    joined.pprint()
    # sink = joined.foreachRDD(lambda rdd: rdd.foreachPartition(KafkaOutput(config).send_partition))
    ssc.start()
    ssc.awaitTermination()
