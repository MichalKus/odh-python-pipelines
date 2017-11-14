import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.multiline_event_creator import MultilineEventCreator
from common.log_parsing.list_event_creator.splitter_parser import SplitterParser
from common.log_parsing.metadata import Metadata, TimestampField, StringField
from util import Utils


def concat(item1, item2): return item1 + item2


def take_first(item1, item2): return item1


def create_event_creators(configuration=None):
    return MatchField("topic", {
        "stagis_log_gen": SourceConfiguration(stagis_ee_event_creator(), Utils.get_output_topic(configuration, "general")),
        "stagis_log_err": SourceConfiguration(stagis_ee_event_creator(), Utils.get_output_topic(configuration, "error")),
        "stagis_corecommit_log": SourceConfiguration(stagis_ee_corecommit_event_creator(), Utils.get_output_topic(configuration, "corecommit")),
        "stagis_interface_log": SourceConfiguration(stagis_ee_interface_event_creator(), Utils.get_output_topic(configuration, "interface")),
        "stagis_wcf_log": SourceConfiguration(stagis_ee_wcf_event_creator(), Utils.get_output_topic(configuration, "wcf"))
    })


def stagis_ee_event_creator():
    return EventCreator(Metadata([
        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
        StringField("level"),
        StringField("instance_name"),
        StringField("thread_id"),
        StringField("class_name"),
        StringField("message")
    ]),
        SplitterParser("|", is_trim=True))


def stagis_ee_corecommit_event_creator():
    return EventCreator(Metadata([
        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
        StringField("level"),
        StringField("instance_name"),
        StringField("causality_id"),
        StringField("thread_id"),
        StringField("class_name"),
        StringField("message")
    ]),
        SplitterParser("|", is_trim=True))


def stagis_ee_interface_event_creator():
    return EventCreator(Metadata([
        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
        StringField("instance_name"),
        StringField("thread_id"),
        StringField("class_name"),
        StringField("message")
    ]),
        SplitterParser("|", is_trim=True))


def stagis_ee_wcf_event_creator():
    return MultilineEventCreator(Metadata([
        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
        StringField("message")
    ]),
        SplitterParser("|", is_trim=True), [take_first, concat])

if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
