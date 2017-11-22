import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.event_creator_tree.multisource_configuration import *
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.multiple_event_creator import MultipleEventCreator
from common.log_parsing.list_event_creator.regexp_parser import RegexpParser
from common.log_parsing.metadata import *
from util.utils import Utils


def create_event_creators(configuration=None):
    return MatchField("topic", {
        "traxis_cassandra_log_gen": SourceConfiguration(
            MultipleEventCreator([
                EventCreator(
                    Metadata([
                        StringField("level"),
                        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        "^.*?\:\s+(\S+)\s\[[^\]]*\]\s+(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S[\s\S]+)")
                ),
                EventCreator(
                    Metadata([
                        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
                        StringField("level"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        "^.*?\:\s(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s(\S+)\s+\[[^\]]*\]\s+(\S[\s\S]+)")
                )
            ]),
            Utils.get_output_topic(configuration, "general")
        ),
        "traxis_cassandra_log_err": SourceConfiguration(
            MultipleEventCreator([
                EventCreator(
                    Metadata([
                        StringField("level"),
                        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        "^.*\:\s.*\:\s(\S+)\s\[[^\]]*\]\s+(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S[\s\S]+)")
                ),
                EventCreator(
                    Metadata([
                        StringField("level"),
                        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        "^.*\:\s(\S+)\s\[[^\]]*\]\s+(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S[\s\S]+)")
                ),
                EventCreator(
                    Metadata([
                        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
                        StringField("level"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        "^.*\:\s(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s(\S+)\s\[[^\]]*\]\s+(\S[\s\S]+)")
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
