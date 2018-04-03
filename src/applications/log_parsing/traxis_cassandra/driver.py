"""Spark driver for parsing message from TRAXIS_CASSANDRA component"""
import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.event_creator_tree.multisource_configuration import *
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.multiple_event_creator import MultipleEventCreator
from common.log_parsing.list_event_creator.parsers.regexp_parser import RegexpParser
from common.log_parsing.metadata import Metadata, StringField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(configuration):
    timezone_name = configuration.property("timezone.name")
    timezones_priority = configuration.property("timezone.priority", "dic")

    return MatchField("topic", {
        "traxis_cassandra_log_gen": SourceConfiguration(
            MultipleEventCreator([
                EventCreator(
                    Metadata([
                        StringField("level"),
                        ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                        StringField("message")
                    ]),
                    RegexpParser(
                        r"^.*?\:\s+(\S+)\s\[[^\]]*\]\s+(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S[\s\S]+)")
                ),
                EventCreator(
                    Metadata([
                        ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                        StringField("level"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        r"^.*?\:\s(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s(\S+)\s+\[[^\]]*\]\s+(\S[\s\S]+)")
                )
            ]),
            Utils.get_output_topic(configuration, "general")
        ),
        "traxis_cassandra_log_err": SourceConfiguration(
            MultipleEventCreator([
                EventCreator(
                    Metadata([
                        StringField("level"),
                        ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                        StringField("message")
                    ]),
                    RegexpParser(
                        r"^.*\:\s.*\:\s(\S+)\s\[[^\]]*\]\s+(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S[\s\S]+)")
                ),
                EventCreator(
                    Metadata([
                        StringField("level"),
                        ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                        StringField("message")
                    ]),
                    RegexpParser(
                        r"^.*\:\s(\S+)\s\[[^\]]*\]\s+(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S[\s\S]+)")
                ),
                EventCreator(
                    Metadata([
                        ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                        StringField("level"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        r"^.*\:\s(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s(\S+)\s\[[^\]]*\]\s+(\S[\s\S]+)")
                )
            ]),
            Utils.get_output_topic(configuration, "error")
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
