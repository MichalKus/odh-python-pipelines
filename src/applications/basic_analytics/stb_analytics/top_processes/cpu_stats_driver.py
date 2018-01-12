from __future__ import print_function

import sys
from pyspark import SparkContext
from kafka import KafkaProducer, errors
import json
import time

from util.utils import Utils
from common.adv_analytics.kafkaUtils import KafkaConnector

config = Utils.load_config(sys.argv[1])
sc = SparkContext(appName=config.property('spark.appName'), master=config.property('spark.master'))
sc.setLogLevel("WARN")

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
    required = ['originId', 'proc_name', 'proc_ts', 'proc_stime', 'proc_utime']
    check = all(field in msg for field in required)
    return check

def format_to_pair(json_msg):
    originId = json_msg['originId']
    proc_name = json_msg['proc_name']
    key = originId+'-'+proc_name
    return (key, json_msg)

def split_grouping(x):
    key = x[0]
    val = x[1]
    if len(val) > 1:
        ts = []
        for x in val:
            ts.append(x['proc_ts'])
        uniq_ts = list(set(ts))
        res = []
        uniq_ts.sort(key=int)
        for t in uniq_ts:
            ix = len(ts) - 1 - ts[::-1].index(t)
            res.append((key, val[ix]))
    else:
        res = [(key, val[0])]

    return res

def prepare_for_state(x):
    key = x[0]
    val = x[1]
    proc_ts = int(val['proc_ts'])

    return (key, (proc_ts, val['proc_stime'], val['proc_utime']))

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
        .map(lambda msg: format_to_pair(msg)) \
        .groupByKey().map(lambda x : (x[0], list(x[1]))) \
        .flatMap(lambda x: split_grouping(x))

    return output

def update_state(new_state, previous_state):
    if previous_state is None:
        return new_state[-1:]
    else:
        state = previous_state[-1:]
        if new_state != []:
            if int(new_state[-1][0]) > int(state[0][0]):
                return state + new_state[-1:]

        return previous_state

def state_manipulate(stream):
    stream.cache()
    return stream \
        .map(lambda x: prepare_for_state(x)) \
        .updateStateByKey(update_state)

def calc_cpu_kpi(msg):
    """
    utime and stime are stored in jiffies (units)
    a view on cpu usage is obtained by calculating the rate
    of increase in utime & stime

    name of kpi: 'proc_stime_rate' & 'proc_utime_rate'
    units: jiffies/sec
    :param msg: rdd pair element (key, (json, state))
    :return: (key, new_json)
    """
    key = msg[0]
    json_msg = msg[1][0]
    state = msg[1][1]
    match = False
    for x in state[::-1]:
        if int(json_msg['proc_ts']) < x[0]:
            match = True
            dt = (x[0] - int(json_msg['proc_ts']))/1000.0
            proc_stime_rate = round(((json_msg['proc_stime'] - x[1]) / dt) * 10000) / 10000.0
            if proc_stime_rate < 0:
                proc_stime_rate = None
            proc_utime_rate = round(((json_msg['proc_utime'] - x[2]) / dt) * 10000) / 10000.0
            if proc_utime_rate < 0:
                proc_utime_rate = None
            break
    if not match:
        proc_stime_rate = None
        proc_utime_rate = None

    json_msg['proc_stime_rate'] = proc_stime_rate
    json_msg['proc_utime_rate'] = proc_utime_rate
    return json_msg

def es_structure(msg):
    doc = {
        'timestamp': msg['proc_ts'],
        'originId': msg['originId'],
        'proc_name': msg['proc_name'],
        'proc_rss': msg['proc_rss'],
        'MemoryUsage_totalKb': msg['MemoryUsage_totalKb'],
        'proc_mem_usage': msg['proc_mem_usage'],
        'proc_stime': msg['proc_stime'],
        'proc_stime_rate': msg['proc_stime_rate'],
        'proc_utime': msg['proc_utime'],
        'proc_utime_rate': msg['proc_utime_rate']
    }
    return doc

def carbon_structure(msg):
    carbon_path = config.property('analytics.componentName')
    mem = {
        'proc_name': msg['proc_name'],
        '@timestamp': msg['proc_ts'],
        'originId': msg['originId'],
        'proc_rss': msg['proc_rss'],
        'MemoryUsage_totalKb': msg['MemoryUsage_totalKb'],
        'proc_mem_usage': msg['proc_mem_usage'],
        'metric_name': '{}.mem.MemoryUsage_percentage'.format(carbon_path),
        'value': msg['proc_mem_usage']
    }
    cpu_stime = {
        'proc_name': msg['proc_name'],
        '@timestamp': msg['proc_ts'],
        'originId': msg['originId'],
        'proc_stime': msg['proc_stime'],
        'proc_stime_rate': msg['proc_stime_rate'],
        'metric_name': '{}.cpu.stime_rate_jiffiespersec'.format(carbon_path),
        'value': msg['proc_stime_rate']
    }
    cpu_utime = {
        'proc_name': msg['proc_name'],
        'timestamp': msg['proc_ts'],
        'originId': msg['originId'],
        'proc_utime': msg['proc_utime'],
        'proc_utime_rate': msg['proc_utime_rate'],
        'metric_name': '{}.cpu.utime_rate_jiffiespersec'.format(carbon_path),
        'value': msg['proc_utime_rate']
    }
    return [mem, cpu_stime, cpu_utime]

def join_streams(stream_1, stream_2):
    joined = stream_1 \
        .leftOuterJoin(stream_2) \
        .map(lambda x: calc_cpu_kpi(x))

    # sink = joined.flatMap(lambda x: carbon_structure(x))
    sink = joined.map(lambda x: es_structure(x))

    return sink

def send_partition(iter):
    topic = config.property('kafka.topicOutput')
    producer = KafkaProducer(bootstrap_servers = config.property('kafka.bootstrapServers').split(','))
    for record in iter:
        try:
            record_metadata = producer.send(topic, json.dumps(record).encode('utf-8')).get(timeout=30)
        except errors.KafkaTimeoutError:
            print("Topic: {}, Failed to deliver message".format(topic))
    producer.flush()

if __name__ == "__main__":

    ssc = KafkaConnector.create_spark_context(config, sc)
    component_type = 'virtual_machine'
    input_stream = KafkaConnector(config).create_kafka_stream(ssc)
    output_stream = process_data(input_stream)
    state = state_manipulate(output_stream)
    joined = join_streams(output_stream, state)
    # joined.pprint()
    sink = joined.foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))
    ssc.start()
    ssc.awaitTermination()
