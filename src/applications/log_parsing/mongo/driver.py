"""Spark driver for parsing message from Mongo"""
import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.parsers.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration, MatchField
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.metadata import Metadata, StringField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(configuration):
    """
    Method creates configured event_creator for logs from Mongo
    :param configuration
    :return: Composite event creator for Mongo
    """

    timezone_name = configuration.property("timezone.name")
    timezones_property = configuration.property("timezone.priority", "idc")

    event_creator = EventCreator(
        Metadata([
            ConfigurableTimestampField("timestamp",  "%Y-%m-%dT%H:%M:%S.%f", timezone_name,
                                       timezones_property, "@timestamp", include_timezone=True),
            StringField("level"),
            StringField("event_type"),
            StringField("thread"),
            StringField("message")
        ]),
        RegexpParser(r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+.\d{4})\s+(?P<level>.*?)\s+"
                     r"(?P<event_type>.*?)\s+\[(?P<thread>.*?)\]\s(?P<message>.*)")
    )

    return MatchField("source", {
        "mongo.log": SourceConfiguration(
            event_creator,
            Utils.get_output_topic(configuration, "mongo_parsed"))
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
