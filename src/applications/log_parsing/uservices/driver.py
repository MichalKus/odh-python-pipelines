"""Spark driver for parsing message from Traxis Frontend component"""
import sys

from common.log_parsing.dict_event_creator.json_parser import JsonParser
from common.log_parsing.dict_event_creator.key_value_parser import KeyValueParser
from common.log_parsing.dict_event_creator.multi_fields_event_creator import MultiFieldsEventCreator
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
from common.log_parsing.dict_event_creator.single_type_event_creator import SingleTypeEventCreator
from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.custom_log_parsing_processor import CustomLogParsingProcessor
from common.log_parsing.dict_event_creator.event_creator import EventCreator, CompositeEventCreator
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration
from common.log_parsing.metadata import StringField, Metadata
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from common.spark_utils.custom_functions import convert_to_underlined
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
        RegexpParser("^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)"), field_to_parse="@timestamp"
    )

    http_url_query_event_creator = SingleTypeEventCreator(StringField(None),
                                                          KeyValueParser("&", "=", keys_mapper=convert_to_underlined),
                                                          field_to_parse="http_urlquery")

    clean_subscriber_id_event_creator = EventCreator(
        Metadata(
            [StringField("clean_subscriber_id")]),
        RegexpParser(
            r"(?P<clean_subscriber_id>^.*)_.*",
            return_empty_dict=True),
        field_to_parse="subscriber_id")

    clean_content_item_id_event_creator = EventCreator(
        Metadata(
            [StringField("clean_content_item_id")]),
        RegexpParser(
            r"^.*telenet.be%2F(?P<clean_content_item_id>.*)",
            return_empty_dict=True),
        field_to_parse="content_item_id")

    clean_lgi_content_item_instance_id = EventCreator(
        Metadata(
            [StringField("clean_lgi_content_item_instance_id")]),
        RegexpParser(
            r"^.*%3A(?P<clean_lgi_content_item_instance_id>.*)",
            return_empty_dict=True),
        field_to_parse="lgi_content_item_instance_id")

    api_methods_event_creator = MultiFieldsEventCreator(StringField("api_method"),
                                                        ["app", "header_x-original-uri"],
                                                        [(["recording-service", "bookings"], "bookings"),
                                                         (["recording-service", "recordings"], "recordings"),
                                                         (["purchase-service", "history"], "history"),
                                                         (["purchase-service", "entitlements"], "entitlements"),
                                                         (["vod-service", "contextualvod"], "contextualvod"),
                                                         (["vod-service", "detailscreen"], "detailscreen"),
                                                         (["vod-service", "gridscreen"], "gridscreen"),
                                                         (["discovery-service", "learn-actions"], "learn-actions"),
                                                         (["discovery-service", "search"], "search"),
                                                         (["discovery-service", "recommendations"], "recommendations"),
                                                         (["session-service", "channels"], "channels"),
                                                         (["session-service", "cpes"], "cpes")])

    return SourceConfiguration(
        CompositeEventCreator()
        .add_source_parser(json_event_creator)
        .add_intermediate_result_parser(timestamp_event_creator)
        .add_intermediate_result_parser(http_url_query_event_creator)
        .add_intermediate_result_parser(clean_subscriber_id_event_creator)
        .add_intermediate_result_parser(clean_content_item_id_event_creator)
        .add_intermediate_result_parser(clean_lgi_content_item_instance_id)
        .add_intermediate_result_parser(api_methods_event_creator, final=True),
        Utils.get_output_topic(config, "uservices_parsed_logs")
    )


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        CustomLogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
