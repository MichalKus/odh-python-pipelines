from __future__ import print_function

import sys
from pyspark import SparkContext
from kafka import KafkaProducer, errors
import json
import time

from util.utils import Utils
from common.adv_analytics.kafkaUtils import KafkaConnector
from required_fields import fields_lookup

config = Utils.load_config(sys.argv[1])
sc = SparkContext(appName=config.property('spark.appName'), master=config.property('spark.master'))
sc.setLogLevel("WARN")
sc.addPyFile('/odh/python/common/adv_analytics/lib/kazoo.zip')

from kazoo.client import KazooClient

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

def json_check(msg):
    try:
        json.loads(msg[1])
    except (ValueError, TypeError) as e:
        return False
    return True

def filter_check(msg):
    """
    check if topic message contains the necessary fields
    :param msg: topic message
    :return: Boolean to indicate whether check is successful.
    """
    required = fields_lookup[component_type]
    check = all(field in msg for field in required)
    return check

def process_data(kafkaStream):
    """
    Take input stream and encode all topic messages into json format.
    Ignore messages that do not contained the required fields.
    Finally strip out undesired fields
    :param kafkaStream: Input stream
    :return: filtered input stream
    """
    output = kafkaStream \
        .filter(json_check) \
        .map(lambda x: json.loads(x[1])) \
        .filter(filter_check) \
        .map(lambda x: strip_fields_virtual_machine(x))
    return output

def update_zookeeper(res, zk):
    if zk.exists("/observer/vm/"+res["name"]):
        zk.set("/observer/vm/"+res["name"], json.dumps(res).encode('utf-8'))
    else:
        zk.create("/observer/vm/"+res["name"], json.dumps(res).encode('utf-8'))

def kafkaSink(topic, msg, producer, zk):
    """
    Send json message into kafka topic. Avoid async operation to guarantee
    msg is delivered.
    """
    try:
        record_metadata = producer.send(topic, json.dumps(msg).encode('utf-8')).get(timeout=30)
        res = {"name": msg["name"], "partition": record_metadata.partition, "offset": record_metadata.offset}
        update_zookeeper(res, zk)
    except errors.KafkaTimeoutError:
        print("Topic: {}, Failed to deliver message: {}".format(topic, str(msg["name"])))

def send_partition(iter):
    topic = config.property('kafka.topicOutput')
    producer = KafkaProducer(bootstrap_servers = config.property('kafka.bootstrapServers').split(','))
    zk = KazooClient(hosts=config.property('kafka.zookeeperHosts').split('/kafka')[0])
    zk.start()
    zk.ensure_path("/observer/vm")
    for record in iter:
        kafkaSink(topic, record, producer, zk)
    producer.flush()
    zk.stop()

if __name__ == "__main__":

    ssc = KafkaConnector.create_spark_context(config, sc)
    component_type = 'virtual_machine'
    input_stream = KafkaConnector(config).create_kafka_stream(ssc)
    output_stream = process_data(input_stream)
    sink = output_stream.foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))
    # output_stream.pprint()
    ssc.start()
    ssc.awaitTermination()
