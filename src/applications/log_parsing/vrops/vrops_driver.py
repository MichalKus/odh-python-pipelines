"""Spark driver for parsing message from VROPS component"""
import sys
import json

import re
from pyspark.sql.functions import from_json, udf, col
from pyspark.sql.types import StringType

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.mutate_event_creator import MutateEventCreator, FieldsMapping
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.composite_event_creator import CompositeEventCreator
from common.log_parsing.dict_event_creator.parsers.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.metadata import Metadata, StringField
from util.utils import Utils


class CustomLogParsingProcessor(LogParsingProcessor):
    """
    Pipeline for custom log parsing for input messages not file from filebeat
    """

    def __init__(self, configuration, event_creators_tree):
        self._LogParsingProcessor__event_creators_tree = event_creators_tree
        self._LogParsingProcessor__dlq_topic = configuration.property("kafka.topics.dlq")
        self.__input_topic = configuration.property("kafka.topics.inputs")[0]

    def udf_format_influx(self, message):
        """
        User defined function for formatting influx line strings to json
        :param message:
        :return: string
        """
        res = {}
        res["message"] = message
        res["topic"] = self.__input_topic
        res["source"] = "VROPS.log"
        res["beat"] = {"hostname": "non-filebeat-msg"}
        return json.dumps(res)

    def create(self, read_stream):
        pre_format_udf = udf(lambda row: self.udf_format_influx(row), StringType())
        create_full_event_udf = udf(lambda row: self._LogParsingProcessor__create_full_event(row),
                                    self._LogParsingProcessor__get_udf_result_schema())
        return [read_stream
                    .withColumn("new_value", pre_format_udf(col('value').cast("string"))) \
                    .select(from_json(col("new_value"), self._LogParsingProcessor__get_message_schema()).alias("json")) \
                    .select(create_full_event_udf("json").alias("result")) \
                    .selectExpr("result.topic AS topic", "result.json AS value")]


def convert_influx_str(metrics):
    """
   convert influx str to dict
   :param metrics: input dict
   """
    pairs = []
    split = re.split(r'[, ]', metrics)
    res = []
    for x in split:
        if '=' in x:
            res.append(x)
        else:
            try:
                res[-1] = res[-1] + x
            except IndexError:
                pass
    for x in res:
        [key, metric_value] = x.split('=')
        try:
            value = float(metric_value)
        except ValueError:
            value = metric_value
        pairs.append((key, value))
    return dict(pairs)


def create_event_creators(configuration):
    """
    Method creates configuration for VROPS Component all metrics
    :param configuration:
    :return: MatchField configuration for VROPS
    """

    custom_dict_event_creator = MutateEventCreator(None, [FieldsMapping(["metrics"], "metrics", convert_influx_str)])

    general_creator = EventCreator(Metadata([
        StringField("group"),
        StringField("name"),
        StringField("res_kind"),
        StringField("metrics"),
        StringField("timestamp")]
    ), RegexpParser(r"(?s)^(?P<group>[-\w]*),.*name=(?P<name>[^,]*).*kind=(?P<res_kind>[^,]*)"
                    r"\s(?P<metrics>.*)\s(?P<timestamp>.*)\n"))

    metrics_creator = EventCreator(Metadata([
        StringField("metrics")]),
        RegexpParser(r"(?s)^(?P<metrics>[^\[^,]+\S+]*)",
                     return_empty_dict=True),
        field_to_parse="metrics")

    return MatchField("source", {
        "VROPS.log": SourceConfiguration(
            CompositeEventCreator()
                .add_source_parser(general_creator)
                .add_intermediate_result_parser(metrics_creator)
                .add_intermediate_result_parser(custom_dict_event_creator)
            ,
            Utils.get_output_topic(configuration, "vrops")
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        CustomLogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
