import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.metadata import Metadata, TimestampField, StringField
from util.utils import Utils


def create_event_creators(configuration=None):
    event_creator = EventCreator(Metadata(
        [TimestampField("timestamp", "%Y-%m-%d %H:%M:%S,%f", "@timestamp"), StringField("level"),
         StringField("thread_name"), StringField("component"), StringField("message")]), RegexpParser(
        "(?s)^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (?P<level>\w+) \[(?P<thread_name>.*?)\] (?P<component>\w+) - (?P<message>.*)$"))

    return MatchField("source", {
        "TraxisService.log": SourceConfiguration(
            event_creator,
            Utils.get_output_topic(configuration, "general")
        ),
        "TraxisServiceError.log": SourceConfiguration(
            event_creator,
            Utils.get_output_topic(configuration, "error")
        ),
        "TraxisServiceDistributedScheduler.log": SourceConfiguration(
            event_creator,
            Utils.get_output_topic(configuration, "scheduler")
        ),
        "TraxisServiceLogManagement.log": SourceConfiguration(
            event_creator,
            Utils.get_output_topic(configuration, "management")
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
