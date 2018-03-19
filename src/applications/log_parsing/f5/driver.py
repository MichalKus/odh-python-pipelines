"""
Spark driver for parsing STB F5 messages
"""

import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.event_creator import CompositeEventCreator, EventCreator
from common.log_parsing.dict_event_creator.key_value_parser import KeyValueParser
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
from common.log_parsing.dict_event_creator.single_type_event_creator import SingleTypeEventCreator
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.metadata import StringField, Metadata
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(config):
    """
    Method to create a list of event creators
    :param config: Job configuration.
    :return: A list of event creators.
    """

    timezone_name = config.property("timezone.name")
    timezones_priority = config.property("timezone.priority", "dic")

    message_general_event_creator = SingleTypeEventCreator(
        StringField(None),
        KeyValueParser(",", "=", values_strip='"')
    )

    request_header_event_creator = SingleTypeEventCreator(
        StringField(None),
        KeyValueParser("\\r\\n", ":", skip_parsing_exceptions=True),
        field_to_parse="request_header"
    )

    timestamp_event_creator = EventCreator(
        Metadata([ConfigurableTimestampField("timestamp", timezone_name, timezones_priority, "@timestamp")]),
        RegexpParser(r"(?P<timestamp>\d+)"), field_to_parse="EOCTimestamp"
    )

    return CompositeEventCreator() \
        .add_source_parser(message_general_event_creator) \
        .add_intermediate_result_parser(request_header_event_creator) \
        .add_intermediate_result_parser(timestamp_event_creator)


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
