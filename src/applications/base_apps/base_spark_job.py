import json
import marshal
import types

from pyspark import SparkContext, RDD
from pyspark.streaming import StreamingContext, DStream
from pyspark.streaming.kafka import KafkaUtils

from common.kafka_helper import KafkaHelper
from common.state import State


class BaseSparkJob(object):
    @staticmethod
    def broadcast(obj, value):
        context = obj if isinstance(obj, SparkContext) else \
            obj.sparkContext if isinstance(obj, StreamingContext) else \
            obj.context if isinstance(obj, RDD) else \
            obj.context().sparkContext if isinstance(obj, DStream) else None

        if context:
            return context.broadcast(value)

        return None

    @staticmethod
    def create_context(config):
        sc = SparkContext(appName=config.property('spark.appName'),
                          master=config.property('spark.master'))
        ssc = StreamingContext(sc, config.property("spark.batchInterval"))
        ssc.checkpoint(config.property("spark.checkpointLocation"))
        return ssc

    @staticmethod
    def extract_data(stream, extractors):
        messages = stream.map(lambda x: json.loads(x[1])).map(lambda j: j['message'])
        if extractors:
            broadcast_extractors = BaseSparkJob.broadcast(stream, extractors)
            return messages \
                .flatMap(lambda e: [extractor.extract(e) for extractor
                                    in broadcast_extractors.value]) \
                .filter(lambda x: x is not None)
        else:
            return messages

    @staticmethod
    def update_values(value, values):
        return values + value

    @staticmethod
    def update_state(config, value, state, update_values):
        if not state:
            return State(update_values(value, []),
                         expired_time=config.property('expired_time'))
        elif state.is_expired():
            return None
        else:
            return state.update(value, update_values)

    @staticmethod
    def is_valid_state(state):
        return state.is_expired

    @staticmethod
    def to_json(config, array):
        result = {}
        for i, item in enumerate(array):
            result["item {}".format(i)] = item
        return result

    def __init__(self, config, context_creator=None, extractors=None):
        self.__config = config
        self.__context = context_creator(config) if context_creator \
            else self.create_context(config)
        self.__extractors = extractors

    def execute(self):
        options = self.__config.kafka_input_options()
        topics = self.__config.kafka_input_topics()
        stream = KafkaUtils.createDirectStream(self.__context, topics, options)
        extracted = self.extract_data(stream, self.__extractors)

        broadcast_bootstrap_servers = self.broadcast(self.__context, self.__config.kafka_bootstrap_servers())
        broadcast_output_topics = self.broadcast(self.__context, self.__config.kafka_output_topics())
        broadcast_config = self.broadcast(self.__context, self.__config)

        broadcast_to_json = self.broadcast(self.__context, marshal.dumps(self.to_json.func_code))
        broadcast_is_valid_state = self.broadcast(self.__context, marshal.dumps(self.is_valid_state.func_code))
        broadcast_update_state = self.broadcast(self.__context, marshal.dumps(self.update_state.func_code))
        broadcast_update_values = self.broadcast(self.__context, marshal.dumps(self.update_values.func_code))

        extracted \
            .updateStateByKey(lambda value, state: types.FunctionType(marshal.loads(broadcast_update_state.value),  globals(), "update_state")(
                broadcast_config.value, value, state,
                types.FunctionType(marshal.loads(broadcast_update_values.value), globals(), "update_values")
            )).filter(
                lambda (package, state): types.FunctionType(marshal.loads(broadcast_is_valid_state.value), globals(), "is_valid_state")(state)
            ).foreachRDD(
                lambda rdd: KafkaHelper.transform_and_send(
                    rdd, lambda x: types.FunctionType(marshal.loads(broadcast_to_json.value), globals(), "to_json")(broadcast_config.value, x),
                    broadcast_bootstrap_servers.value, broadcast_output_topics.value)
            )

        self.__context.start()
        self.__context.awaitTermination()
