from __future__ import print_function

import sys
from pyspark import SparkContext
from util.utils import Utils
from common.adv_analytics.kafkaUtils import KafkaConnector
import json
import requests
import datetime

def json_check(msg):
    try:
        json.loads(msg[1])
    except (ValueError, TypeError) as e:
        return False
    return True

def prepare_doc(msg):
    """
    Add timestamp
    :param msg: input stream message
    :return: document for export to ES
    """
    doc = msg.copy()
    doc["timestamp"] = str(datetime.datetime.now())
    return doc

def read_data(kafka_stream):
    """
    Take input stream and encode all topic messages into json format.
    Ignore messages that do not contained the required fields.
    :param kafkaStream: Input stream
    :return: filtered input stream
    """
    rdd_stream = kafka_stream \
        .filter(json_check) \
        .map(lambda x: json.loads(x[1])) \
        .map(lambda msg: prepare_doc(msg))
    return rdd_stream

def es_sink(doc):
    """
    Export to ES
    :param doc: message
    """
    headers = {
        "content-type": "application/json"
    }
    url = "http://{}/{}/obo/{}".format(config.property('es.host'), config.property('es.index'), doc["tenant"])
    try:
        r = requests.request("PUT", url, data=json.dumps(doc), headers=headers)
    except requests.ConnectionError:
        print("ERROR::SPARK-CloudmapIngestionProcessing::Connection Error with ES")

def send_partition(iter):
    for record in iter:
        es_sink(record)

if __name__ == "__main__":
    config = Utils.load_config(sys.argv[1])
    sc = SparkContext(appName=config.property('spark.appName'), master=config.property('spark.master'))
    sc.setLogLevel("WARN")
    ssc = KafkaConnector.create_spark_context(config, sc)
    input_stream = KafkaConnector(config).create_kafka_stream(ssc)
    output_stream = read_data(input_stream)
    sink = output_stream.foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))
    ssc.start()
    ssc.awaitTermination()
