from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from common.kafka_helper import KafkaHelper
import json
import time
from util.utils import Utils
from required_fields import fields_lookup
import argparse

def strip_fields_virtual_machine(msg):
    """
    Extract the necessary fields from the message and convert datetime to timestamp.
    :param msg: dict from topic message
    :return: result: dict representing the new topic message.
    """
    required = fields_lookup[component_type]
    result = dict((k, msg[k]) for k in tuple(required))
    result['timestamp'] = int(time.mktime(time.strptime(msg['datetime'], '%Y-%m-%dT%H:%M:%SZ')))

    return result

def filter_check(msg):
    """
    check if topic message contains the necessary fields
    :param msg: topic message
    :return: Boolean to indicate whether check is successful.
    """
    required = fields_lookup[component_type]
    check = all(field in msg for field in required)

    return check

def create_spark_context(config):
    """
    Create the spark context using the correct configurations
    :param config: configurations from .yml file
    :return: spark context
    """
    sc = SparkContext(appName=config.property('spark.appName'), master=config.property('spark.master'))
    ssc = StreamingContext(sc, config.property('spark.batchInterval'))
    # ssc.checkpoint(config.property('spark.checkpointLocation'))

    return ssc

def create_kafka_stream(ssc, config):
    """Create Kafka Stream."""
    options = config.kafka_input_options()
    topics = config.kafka_input_topics()
    # input_stream = KafkaUtils.createDirectStream(ssc, topics, options)
    topics = {topics[0]: config.property('kafka.partitions')}
    zookeeper = config.property('kafka.zookeeperHosts').split(",")[0]
    group_id = config.property('kafka.groupId')
    input_stream = KafkaUtils.createStream(ssc, zookeeper, group_id, topics)

    return input_stream

def read_data(kafkaStream):
    """
    Take input stream and encode all topic messages into json format.
    Ignore messages that do not contained the required fields.
    :param kafkaStream: Input stream
    :return: filtered input stream
    """
    msg = kafkaStream \
        .map(lambda x: json.loads(x[1])) \
        .filter(filter_check)

    return msg

def send_data(config, stream, transformer):
    """
    Transform the input stream and return an output stream which is sent to kafka topic
    :param config: configurations from file
    :param stream: input stream
    :param transformer: method used to transform message into the required format.
    :return: output stream
    """
    bootstrap_servers = config.kafka_bootstrap_servers()
    bootstrap_server = bootstrap_servers.split[','][0]
    topics = config.kafka_output_topics()
    output_stream = stream \
        .foreachRDD(lambda rdd: KafkaHelper
                    .transform_and_send(rdd, transformer, bootstrap_server, topics))

    return output_stream

if __name__ == "__main__":
    config = Utils.load_config(sys.argv[1])
    component_type = 'virtual_machine'
    ssc = create_spark_context(config)
    input_stream = read_data(create_kafka_stream(ssc, config))
    output_stream = send_data(config, input_stream, strip_fields_virtual_machine)
    # input_stream.pprint()
    ssc.start()
    ssc.awaitTermination()
