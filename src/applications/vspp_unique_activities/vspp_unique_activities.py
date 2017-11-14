import json
import re

import sys
import uuid

from datetime import datetime, timedelta
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from applications.vspp_unique_activities.recording_state import RecordingState
from common.kafka_helper import KafkaHelper
from util.utils import Utils

if __name__ == "__main__":

    config = Utils.load_config(sys.argv[:])

    sc = SparkContext(appName=config.property('spark.appName'),
                      master=config.property('spark.master'))

    ssc = StreamingContext(sc, config.property('spark.batchInterval'))
    ssc.checkpoint(config.property('spark.checkpointLocation'))

    # Kafka input configs
    options = config.kafka_input_options()
    input_stream = KafkaUtils.createDirectStream(ssc, config.kafka_input_topics(), options)


    def parse_message(message):

        # Get timestamp from message
        timestamp_search = re.search('^(\d{2}\/\d{2}\/\d{2}\s\d{2}:\d{2}:\d{2})', message, re.I)
        timestamp = None
        if timestamp_search:
            timestamp = datetime.strptime(timestamp_search.group(1), '%m/%d/%y %H:%M:%S')
            timestamp.replace(second=0)

        # Get session_name from message
        session_name_search = re.search('^(\d{2}\/\d{2}\/\d{2}\s\d{2}:\d{2}:\d{2}).*started\snew\srecording\sfor\sasset\s(.*)_abr', message, re.I)
        session_name = ''
        action_type = ''
        if session_name_search:
            session_name = session_name_search.group(2)
            action_type = 'started'
        else:
            session_name_search = re.search('^(\d{2}\/\d{2}\/\d{2}\s\d{2}:\d{2}:\d{2}).*Finished\srecording\sasset\s(.*),\s\d+\ssuccessful', message, re.I)
            if session_name_search:
                session_name = session_name_search.group(2)
                action_type = 'finished'

        return session_name, (timestamp, session_name, action_type)

    def generate_timestamps_per_delta(start, end, delta):
        curr = start
        while curr <= end:
            yield curr
            curr += delta

    def generate_messages(element):
        session_name = element[0] + '-' + str(uuid.uuid4())
        started = [item for item in element[1].values() if item[2] == 'started']
        finished = [item for item in element[1].values() if item[2] == 'finished']

        if started and finished:
            min_timestamp = started[0][0]
            max_timestamp = finished[0][0]
            return [(session_name, i) for i in generate_timestamps_per_delta(min_timestamp, max_timestamp, timedelta(minutes=config.property('pipeline.frequencyMinutes')))]\


    def update_values(value, values=None):
        if values is None:
            values = []

        values = values + value

        started = [item for item in values if item[2] == 'started']
        finished = [item for item in values if item[2] == 'finished']

        started.sort(key=lambda i: i[0])
        finished.sort(key=lambda i: i[0], reverse=True)

        started_value = [started[0]] if started else []
        finished_value = [finished[0]] if finished else []

        return started_value + finished_value


    def prepare_session_name(event):
        session_name = event[0]
        timestamp = event[1]

        return {
            '@timestamp': timestamp.isoformat(),
            'system': 'Unique Activities',
            'session': session_name
        }

    input_stream \
        .map(lambda x: json.loads(x[1])) \
        .map(lambda j: j['message']) \
        .map(parse_message) \
        .filter(lambda m: m[0] != '') \
        .updateStateByKey(RecordingState.updateStateIfNotExpired) \
        .filter(lambda (session_name, state): RecordingState.isValidState(state)) \
        .flatMap(lambda e: generate_messages(e)) \
        .foreachRDD(lambda rdd: KafkaHelper.transform_and_send(
            rdd, prepare_session_name,
            config.kafka_bootstrap_servers(),
            config.kafka_output_topics()))

    ssc.start()
    ssc.awaitTermination()
