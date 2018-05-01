"""Spark driver for parsing message from Traxis Frontend component"""
import sys
from datetime import datetime

from common.log_parsing.dict_event_creator.event_with_url_creator import EventWithUrlCreator
from common.log_parsing.dict_event_creator.parsers.json_parser import JsonParser
from common.log_parsing.dict_event_creator.predicate_event_creator import PredicateEventCreator
from common.log_parsing.dict_event_creator.parsers.regexp_parser import RegexpParser
from common.log_parsing.dict_event_creator.single_type_event_creator import SingleTypeEventCreator
from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.mutate_event_creator import MutateEventCreator, FieldsMapping
from common.log_parsing.custom_log_parsing_processor import CustomLogParsingProcessor
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.composite_event_creator import CompositeEventCreator
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration
from common.log_parsing.metadata import StringField, Metadata
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils

def calculate_timestamp(x, y):
    """
    Calculate present timestamp by adding startTime & elapsedTime
    Convert epoch timestamp (ms) to datetime
    """
    return datetime.utcfromtimestamp(float(x + y)/1000)

def create_event_creators(config):
    """
    Method creates configuration for UServices Component
    :param config, configuration
    :return: Composite event creator for UServices
    """

    json_event_creator = SingleTypeEventCreator(StringField(None),
                                                JsonParser(keys_mapper=None, values_mapper=None, flatten=True,
                                                           delimiter='_', fields_to_flat=["state", "licensing"]))

    timestamp_event_creator = MutateEventCreator(None, [FieldsMapping(["startTime", "elapsedTime"], "@timestamp", calculate_timestamp)])

    tenant_crid_creator = EventCreator(Metadata([
        StringField("country"),
        StringField("crid")]),
        RegexpParser(r"^.*Countries/(?P<country>[^/]*).*(?P<crid>crid[^/]*)",
                     return_empty_dict=True),
        field_to_parse="inputs/0/url")

    return SourceConfiguration(
        CompositeEventCreator()
        .add_source_parser(json_event_creator)
        .add_intermediate_result_parser(timestamp_event_creator)
        .add_intermediate_result_parser(tenant_crid_creator, final=True),
        Utils.get_output_topic(config, "transcoder_job")
    )


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        CustomLogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
