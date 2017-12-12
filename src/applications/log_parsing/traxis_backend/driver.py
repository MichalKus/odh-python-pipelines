import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.event_creator_tree.multisource_configuration import *
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.regexp_parser import RegexpParser
from common.log_parsing.metadata import *
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(configuration):
    timezone_name = configuration.property("timezone.name")
    timezones_priority = configuration.property("timezone.priority", "dic")

    return MatchField("topic", {
        "traxis_backend_log_gen": MatchField("source", {
            "TraxisService.log": SourceConfiguration(
                EventCreator(
                    Metadata([
                        ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                        StringField("level"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        "^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S+)\s+\[[^\]]+\]\s+([\s\S]*)")
                ),
                Utils.get_output_topic(configuration, "general")
            ),
            "TraxisServiceDistributedScheduler.log": SourceConfiguration(
                EventCreator(
                    Metadata([
                        ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                        StringField("level"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        "^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S+)\s+\[[^\]]+\]\s+([\s\S]*)")
                ),
                Utils.get_output_topic(configuration, "scheduler")
            ),
            "TraxisServiceLogManagement.log": SourceConfiguration(
                EventCreator(
                    Metadata([
                        ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                        StringField("level"),
                        StringField("message")
                    ]),
                    RegexpParser(
                        "^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S+)\s+\[[^\]]+\]\s+([\s\S]*)")
                ),
                Utils.get_output_topic(configuration, "management")
            )
        }),
        "traxis_backend_log_err": SourceConfiguration(
            EventCreator(
                Metadata([
                    ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                    StringField("level"),
                    StringField("message")
                ]),
                RegexpParser(
                    "^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S+)\s+\[[^\]]+\]\s+([\s\S]*)")
            ),
            Utils.get_output_topic(configuration, "error")
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
