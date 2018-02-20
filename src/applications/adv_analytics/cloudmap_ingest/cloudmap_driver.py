from __future__ import print_function

import sys
from pyspark import SparkContext, SparkConf
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

def flatten(msg):
    """
    flatten nested json into lists
    :param msg:
    :return:
    """
    res = []
    for tn in msg[1]:
        tenant = tn['tenant']
        for epg in tn['epg']:
            for endpoint in tn['epg'][epg]:
                res.append('{},{},{}'.format(tenant, epg, endpoint['virtual_machine']))
    return res

def export_to_hdfs(input_stream):
    """

    :param input_stream:
    :return:
    """
    output = input_stream \
        .map(lambda x: ('hdfs', [json.loads(x[1])])) \
        .reduceByKey(lambda x, y: x + y) \
        .flatMap(lambda msg: flatten(msg))

    return output

def hdfs_sink(rdd):
    """
    Save rdd to HDFS
    :param rdd:
    :return:
    """
    if not len(rdd.take(1)) == 0:
        rdd.repartition(1).saveAsTextFile("file:///spark/checkpoints/cloudmap/cloudmap_mapping2.csv")

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
    myConf = SparkConf().setAppName(config.property('spark.appName')).setMaster(config.property('spark.master')).set(
        "spark.hadoop.validateOutputSpecs", "false")
    sc = SparkContext(conf=myConf)
    sc.setLogLevel("WARN")
    ssc = KafkaConnector.create_spark_context(config, sc)

    input_stream = KafkaConnector(config).create_kafka_stream(ssc)
    hdfs_stream = export_to_hdfs(input_stream)
    hdfs_stream.foreachRDD(lambda rdd: hdfs_sink(rdd))
    es_stream = read_data(input_stream)
    sink = es_stream.foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))
    ssc.start()
    ssc.awaitTermination()
