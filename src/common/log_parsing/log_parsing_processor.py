from common.log_parsing.metadata import ParsingException

from pyspark.sql.types import *
from pyspark.sql.functions import *

import json
from datetime import date, datetime


class LogParsingProcessor:
    """
    Pipeline for log parsing
    """

    def __init__(self, configuration, event_creators_tree):
        self.__event_creators_tree = event_creators_tree
        self.__dlq_topic = configuration.property("kafka.topics.dlq")

    def create(self, read_stream):
        create_full_event_udf = udf(self.__create_full_event, self.__get_udf_result_schema())
        return [read_stream.select(
            from_json(read_stream["value"].cast("string"), self.__get_message_schema()).alias("json")) \
                    .select(create_full_event_udf("json").alias("result")) \
                    .selectExpr("result.topic AS topic", "result.json AS value")]

    @staticmethod
    def __get_message_schema():
        return StructType([
            StructField("beat", StructType([
                StructField("hostname", StringType())
            ])),
            StructField("topic", StringType()),
            StructField("source", StringType()),
            StructField("message", StringType())
        ])

    @staticmethod
    def __get_udf_result_schema():
        return StructType([
            StructField("topic", StringType()),
            StructField("json", StringType())
        ])

    def __create_full_event(self, row):
        try:
            source_configuration = self.__event_creators_tree.get_parsing_context(row)
            result = source_configuration.event_creator.create(row)
            topic = source_configuration.output_topic
        except ParsingException as exception:
            topic = self.__dlq_topic
            result = {"message": row.message, "reason": exception.message}
        result.update({"hostname": row.beat.hostname, "environment": row.topic, "source": row.source})
        return topic, json.dumps(result, default=self.__json_serial)

    def __get_parsing_objects(self, row):
        return self.__event_creators_tree.get_parsing_objects(row)

    @staticmethod
    def __json_serial(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        raise TypeError("Type %s not serializable" % type(value))
