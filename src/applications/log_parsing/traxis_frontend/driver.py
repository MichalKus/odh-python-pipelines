"""Spark driver for parsing message from Traxis Frontend component"""
import sys

from common.log_parsing.dict_event_creator.single_type_event_creator import SingleTypeEventCreator
from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.parsers.key_value_parser import KeyValueParser
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.composite_event_creator import CompositeEventCreator
from common.log_parsing.dict_event_creator.parsers.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.matchers.matcher import SubstringMatcher
from common.log_parsing.metadata import Metadata, StringField, IntField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(configuration):
    """
    Method creates configuration for Traxis Frontend Component
    :param configuration
    :return: MatchField configuration for Traxis Frontend
    """
    timezone_name = configuration.property("timezone.name")
    timezones_priority = configuration.property("timezone.priority", "dic")

    event_creator = EventCreator(Metadata([
        ConfigurableTimestampField("timestamp", timezone_name, timezones_priority, "@timestamp"),
        StringField("level"),
        StringField("thread_name"),
        StringField("component"),
        StringField("message")]),
        RegexpParser(r"(?s)^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})"
                     r"\s*"
                     r"(?P<level>\w+)"
                     r"\s*"
                     r"\[(?P<thread_name>.*?)\]"
                     r"\s*"
                     r"(?P<component>\w+)"
                     r"\s*-\s*"
                     r"(?P<message>.*)$"))

    ip_event_creator = EventCreator(
        Metadata(
            [StringField("ip")]),
        RegexpParser(
            r"^\[(?P<ip>[0-9,\.: ]*?)\].*",
            return_empty_dict=True))

    request_id_event_creator = EventCreator(
        Metadata(
            [StringField("request_id", "request-id")]),
        RegexpParser(
            r".*\[RequestId = (?P<request_id>.*?)\].*",
            return_empty_dict=True))

    obo_customer_id_event_creator = EventCreator(
        Metadata(
            [StringField("obo_customer_id", "obo-customer-id")]),
        RegexpParser(
            r".*\[CustomerId = (?P<obo_customer_id>.*?)\].*",
            return_empty_dict=True))

    x_request_id_event_creator = EventCreator(
        Metadata(
            [StringField("x_request_id", "x-request-id")]),
        RegexpParser(
            r".*x-request-id: (?P<x_request_id>[a-z0-9- ]*).*",
            return_empty_dict=True))

    method_duration_event_creator = EventCreator(
        Metadata(
            [StringField("method"),
             StringField("duration")]),
        RegexpParser(
            r"^.*Executing method \'(?P<method>.*?)\' took \'(?P<duration>.*?)\'.*",
            return_empty_dict=True),
        matcher=SubstringMatcher("Executing method"))

    method_invoked_event_creator = EventCreator(
        Metadata(
            [StringField("method"),
             StringField("identity"),
             StringField("product_id", "productId")]),
        RegexpParser(
            r"^.*Method \'(?P<method>.*?)\' invoked with parameters\: identity = (?P<identity>.*?)\, productId ="
            r" (?P<product_id>.*?)(\,.*|$)",
            return_empty_dict=True),
        matcher=SubstringMatcher("invoked with parameters"))

    cannot_purchase_product_event_creator = EventCreator(
        Metadata(
            [StringField("product_id", "productId")]),
        RegexpParser(
            r"^.*Cannot purchase products of type \'Subscription\'.*productId \'(?P<product_id>.*?)\'$",
            return_empty_dict=True),
        matcher=SubstringMatcher("Cannot purchase products of type"))

    query_metrics_event_creator = EventCreator(
        Metadata(
            [StringField("query_metrics")]),
        RegexpParser(
            r"^.*QueryMetrics:(?P<query_metrics>.*)",
            return_empty_dict=True),
        matcher=SubstringMatcher("QueryMetrics"))

    key_value_event_creator = SingleTypeEventCreator(IntField(None),
                                                     KeyValueParser(",", "="),
                                                     field_to_parse="query_metrics")

    id_event_creator = CompositeEventCreator() \
        .add_source_parser(event_creator) \
        .add_intermediate_result_parser(ip_event_creator) \
        .add_intermediate_result_parser(request_id_event_creator) \
        .add_intermediate_result_parser(obo_customer_id_event_creator) \
        .add_intermediate_result_parser(x_request_id_event_creator)

    return MatchField("source", {
        "TraxisService.log": SourceConfiguration(
            id_event_creator
            .add_intermediate_result_parser(query_metrics_event_creator)
            .add_intermediate_result_parser(key_value_event_creator, final=True)
            .add_intermediate_result_parser(method_duration_event_creator, final=True)
            .add_intermediate_result_parser(method_invoked_event_creator, final=True)
            .add_intermediate_result_parser(cannot_purchase_product_event_creator, final=True),
            Utils.get_output_topic(configuration, "general")
        ),
        "TraxisServiceError.log": SourceConfiguration(
            id_event_creator,
            Utils.get_output_topic(configuration, "error")
        ),
        "TraxisServiceDistributedScheduler.log": SourceConfiguration(
            id_event_creator,
            Utils.get_output_topic(configuration, "scheduler")
        ),
        "TraxisServiceLogManagement.log": SourceConfiguration(
            id_event_creator,
            Utils.get_output_topic(configuration, "management")
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
