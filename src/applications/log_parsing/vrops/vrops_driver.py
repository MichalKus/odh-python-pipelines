import sys

from util.kafka_pipeline_helper import start_log_parsing_pipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.dict_event_creator.event_creator import EventCreator, CompositeEventCreator
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.metadata import Metadata, StringField
from applications.log_parsing.vrops.metrics_event_creator import MetricsEventCreator
from util.utils import Utils


def create_event_creators(configuration):
    """
    Method creates configuration for VROPS Component all metrics
    :param configuration:
    :return: MatchField configuration for VROPS
    """

    general_creator = EventCreator(Metadata([
        StringField("group"),
        StringField("name"),
        StringField("res_kind"),
        StringField("metrics"),
        StringField("timestamp")]),
        RegexpParser(
            r"(?s)^(?P<group>.*),name=(?P<name>.*),res_kind=(?P<res_kind>[^\,]*)\s(?P<metrics>.*)\s(?P<timestamp>.*)"))

    metrics_creator = MetricsEventCreator(Metadata([
        StringField("metrics")]),
        RegexpParser(r"(?s)^(?P<metrics>[^\[^,]+\S+]*)",
                     return_empty_dict=True),
        field_to_parse="metrics")

    return MatchField("source", {
        "VROPS.log": SourceConfiguration(
            CompositeEventCreator()
                .add_source_parser(general_creator)
                .add_intermediate_result_parser(metrics_creator),
            Utils.get_output_topic(configuration, "vrops")
        )
    })

if __name__ == "__main__":

    
    start_log_parsing_pipeline(create_event_creators)
