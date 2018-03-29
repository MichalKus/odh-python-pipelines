"""
Spark driver for parsing STB F5 messages
"""

import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.event_creator import CompositeEventCreator, EventCreator
from common.log_parsing.dict_event_creator.key_value_parser import KeyValueParser
from common.log_parsing.dict_event_creator.long_timestamp_parser import LongTimestampParser
from common.log_parsing.dict_event_creator.single_type_event_creator import SingleTypeEventCreator
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration
from common.log_parsing.flume_log_parsing_processor import FlumeLogParsingProcessor
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
        KeyValueParser(",", "=", values_mapper=lambda v: v.strip('"'))
    )

    request_header_event_creator = SingleTypeEventCreator(
        StringField(None),
        KeyValueParser("\\r\\n", ":", skip_parsing_exceptions=True),
        field_to_parse="request_header"
    )

    timestamp_event_creator = EventCreator(
        Metadata([ConfigurableTimestampField("timestamp", timezone_name, timezones_priority, "@timestamp")]),
        LongTimestampParser("timestamp"), field_to_parse="eoc_timestamp"
    )

    return SourceConfiguration(
        CompositeEventCreator()
        .add_source_parser(message_general_event_creator)
        .add_intermediate_result_parser(request_header_event_creator)
        .add_intermediate_result_parser(timestamp_event_creator),
        Utils.get_output_topic(config, 'f5_general')
    )


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        FlumeLogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
