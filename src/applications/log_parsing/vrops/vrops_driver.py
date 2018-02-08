# import sys
# import re
# import json
# from common.kafka_pipeline import KafkaPipeline
# from util.utils import Utils
# from pyspark.sql.functions import lit, col, udf
# from pyspark.sql.types import StringType
#
# class VropsParser(object):
#     """
#     Parse VROPS data from influx line protocol form to json
#     """
#
#     def __init__(self, configuration):
#
#         self.__configuration = configuration
#         self.kafka_output = configuration.property("kafka.topics.output")
#
#     def create(self, read_stream):
#         """
#         Create final stream to output to kafka
#         :param read_stream:
#         :return: Kafka stream
#         """
#         json_stream = read_stream \
#             .select(read_stream["value"].cast("string").alias("influx_line"))
#
#         return self._process_pipeline(json_stream)
#
#     def udf_format_influx(self, row):
#         """
#         User defined function for formatting influx line strings to json
#         :param row:
#         :return: string
#         """
#         arr = re.compile(r"[^,\s]+").findall(row)
#         if len(arr) > 3:
#             res = {
#                 "timestamp": arr[-1],
#                 "group": arr[0],
#                 "res_kind": arr[2].split('=')[-1],
#                 "name": arr[1].split('=')[-1]
#             }
#             other = []
#             for x in arr[3:-1]:
#                 if '=' in x:
#                     [key, val] = x.split('=')
#                     try:
#                         metric = (key, float(val))
#                     except ValueError:
#                         metric = (key, val)
#                     other.append(metric)
#             res.update(dict(other))
#             return json.dumps(res)
#         else:
#             return ""
#
#     def _process_pipeline(self, stream):
#         """
#         Pipeline method
#         :param stream: input
#         :return: list of output streams
#         """
#         add_mem_usage = udf(lambda row: self.udf_format_influx(row), StringType())
#         parsed = stream \
#             .withColumn("value", add_mem_usage(col('influx_line'))) \
#             .select(col("value")) \
#             .filter(col("value") != "") \
#             .withColumn("topic", lit(self.kafka_output))
#
#         return [parsed]
#
# def create_processor(configuration):
#     """
#     Build processor using configurations
#     :param configuration: config dictionary
#     """
#     return VropsParser(configuration)
#
#
# if __name__ == "__main__":
#     configuration = Utils.load_config(sys.argv[:])
#     KafkaPipeline(
#         configuration,
#         create_processor(configuration)
#     ).start()

import sys
from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.metadata import Metadata, StringField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from applications.log_parsing.vrops.dictionary_field import CustomDictField
from util.utils import Utils

def create_event_creators(configuration):
    """
    disk,name=lg-l-s-uxp00012,res_kind=VirtualMachine usage_average=6.5333333015441895 1517835494431
    :param configuration:
    :return:
    """
    timezone_name = configuration.property("timezone.name")
    timezones_priority = configuration.property("timezone.priority", "dic")

    event_creator = EventCreator(Metadata([
        # ConfigurableTimestampField("timestamp", timezone_name, timezones_priority, "@timestamp"),
        StringField("group"),
        StringField("name"),
        StringField("res_kind"),
        CustomDictField("metrics"),
        StringField("timestamp")]),
        # ConfigurableTimestampField("timestamp", timezone_name, timezones_priority, "timestamp")]),
        RegexpParser(r"(?s)^(?P<group>.*),name=(?P<name>.*),res_kind=(?P<res_kind>[^\,]*)\s(?P<metrics>.*)\s(?P<timestamp>.*)"))

    return MatchField("source", {
        "VROPS.log": SourceConfiguration(
            event_creator,
            Utils.get_output_topic(configuration, "vrops")
        )
    })

if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
