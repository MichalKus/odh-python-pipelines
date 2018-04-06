"""
    Spark driver for parsing message from CDN component
"""
import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.composite_event_creator import CompositeEventCreator
from common.log_parsing.dict_event_creator.mutate_event_creator import MutateEventCreator, \
    FieldsMapping
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration
from common.log_parsing.list_event_creator.parsers.splitter_parser import SplitterParser
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.metadata import Metadata, StringField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(configuration=None):
    """
    Method creates configuration for CDN
    :param configuration
    :return: Composite event creator for CDN
    """

    timezone_name = configuration.property("timezone.name")
    timezones_property = configuration.property("timezone.priority", "dic")

    cdn_log = EventCreator(Metadata([
        StringField("s_dns"),
        StringField("date"),
        StringField("time"),
        StringField("x_duration"),
        StringField("c_ip"),
        StringField("c_port"),
        StringField("c_vx_zone"),
        StringField("c_vx_gloc"),
        StringField("unknown_field1"),
        StringField("unknown_field2"),
        StringField("cs_method"),
        StringField("cs_uri"),
        StringField("cs_version"),
        StringField("cs_user_agent"),
        StringField("cs_refer"),
        StringField("cs_cookie"),
        StringField("cs_range"),
        StringField("cs_status"),
        StringField("s_cache_status"),
        StringField("sc_bytes"),
        StringField("sc_stream_bytes"),
        StringField("sc_dscp"),
        StringField("s_ip"),
        StringField("s_vx_rate"),
        StringField("s_vx_rate_status"),
        StringField("s_vx_serial"),
        StringField("rs_stream_bytes"),
        StringField("rs_bytes"),
        StringField("cs_vx_token"),
        StringField("sc_vx_download_rate"),
        StringField("x_protohash"),
        StringField("additional_headers"),
        StringField("unknown_field3"),
        StringField("unknown_field4")]),
        SplitterParser("\t", is_trim=True))

    cdn_log_with_timestamp = MutateEventCreator(Metadata([
        ConfigurableTimestampField("timestamp", timezone_name, timezones_property, "@timestamp")]),
        [FieldsMapping(["date", "time"], "timestamp", lambda x, y: x + " " + y, True)])

    return SourceConfiguration(
        CompositeEventCreator()
            .add_source_parser(cdn_log)
            .add_intermediate_result_parser(cdn_log_with_timestamp),
        Utils.get_output_topic(configuration, "cdn_log")
    )


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
