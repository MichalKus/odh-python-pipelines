import json
import marshal
import types

from kafka import KafkaProducer


class KafkaHelper(object):
    @staticmethod
    def transform_and_send(rdd, transformer, bootstrap_servers, topics):
        def send_values(elements):
            producer = KafkaProducer(bootstrap_servers=[bootstrap_servers],
                                     value_serializer=json.dumps)

            transformation = transformer if callable(transformer) else None
            if transformation is None:
                transformation = \
                    types.FunctionType(marshal.loads(transformer.value), globals(), "transform")

            output_topics = topics if not isinstance(topics, basestring) else [topics]
            for element in elements:
                transformed_element = transformation(element)
                if transformed_element:
                    for topic in output_topics:
                        producer.send(topic, transformed_element)

            producer.flush()

        rdd.foreachPartition(send_values)
