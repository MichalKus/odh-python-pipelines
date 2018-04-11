"""Spark driver for parsing message from Catalina component"""
import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.parsers.splitter_parser import SplitterParser
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.metadata import Metadata, StringField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(configuration):

    """
    Method creates configuration for Catalina
    :param configuration
    :return: Composite event creator for Catalina
    """

    timezone_name = configuration.property("timezone.name")
    timezones_property = configuration.property("timezone.priority", "dic")

    event_creator = EventCreator(
        Metadata([
            ConfigurableTimestampField("timestamp", timezone_name, timezones_property, "@timestamp"),
            StringField("level"),
            StringField("message")
        ]),
        SplitterParser(" ", True, 2)
    )

    return SourceConfiguration(
        event_creator,
        Utils.get_output_topic(configuration, "catalina_parsed")
    )


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
