"""Spark driver for parsing message from Traxis Frontend component"""
import sys

from common.log_parsing.dict_event_creator.event_with_url_creator import EventWithUrlCreator
from common.log_parsing.dict_event_creator.parsers.json_parser import JsonParser
from common.log_parsing.dict_event_creator.predicate_event_creator import PredicateEventCreator
from common.log_parsing.dict_event_creator.parsers.regexp_parser import RegexpParser
from common.log_parsing.dict_event_creator.single_type_event_creator import SingleTypeEventCreator
from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.custom_log_parsing_processor import CustomLogParsingProcessor
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.composite_event_creator import CompositeEventCreator
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration
from common.log_parsing.metadata import StringField, Metadata
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(config):
    """
    Method creates configuration for UServices Component
    :param config, configuration
    :return: Composite event creator for UServices
    """

    timezone_name = config.property("timezone.name")
    timezones_priority = config.property("timezone.priority", "dic")

    json_event_creator = SingleTypeEventCreator(StringField(None),
                                                JsonParser(keys_mapper=None, values_mapper=None, flatten=True,
                                                           delimiter='_', fields_to_flat=["http", "header"]))

    timestamp_event_creator = EventCreator(
        Metadata(
            [ConfigurableTimestampField("timestamp", timezone_name, timezones_priority, "@timestamp")]),
        RegexpParser(r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)"), field_to_parse="@timestamp"
    )

    http_url_query_event_creator = EventWithUrlCreator(url_query_field="http_urlquery", delete_source_field=False)

    api_methods_event_creator = PredicateEventCreator(
        ["app", "header_x-original-uri"],
        [(["recording-service", "bookings"], {"api_method": "bookings"}),
         (["recording-service", "recordings"], {"api_method": "recordings"}),
         (["purchase-service", "history"], {"api_method": "history"}),
         (["purchase-service", "entitlements"], {"api_method": "entitlements"}),
         (["vod-service", "contextualvod"], {"api_method": "contextualvod"}),
         (["vod-service", "detailscreen"], {"api_method": "detailscreen"}),
         (["vod-service", "gridscreen"], {"api_method": "gridscreen"}),
         (["discovery-service", "learn-actions"], {"api_method": "learn-actions"}),
         (["discovery-service", "search"], {"api_method": "search"}),
         (["discovery-service", "recommendations"], {"api_method": "recommendations"}),
         (["session-service", "channels"], {"api_method": "channels"}),
         (["session-service", "cpes"], {"api_method": "cpes"})])

    return SourceConfiguration(
        CompositeEventCreator()
        .add_source_parser(json_event_creator)
        .add_intermediate_result_parser(timestamp_event_creator)
        .add_intermediate_result_parser(http_url_query_event_creator)
        .add_intermediate_result_parser(api_methods_event_creator, final=True),
        Utils.get_output_topic(config, "uservices_parsed_logs")
    )


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        CustomLogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
