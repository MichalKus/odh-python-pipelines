import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.list_event_creator.multiple_event_creator import MultipleEventCreator
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.event_creator_tree.multisource_configuration import *
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.regexp_parser import RegexpParser
from common.log_parsing.metadata import Metadata, StringField, IntField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(config):
    """
    Method to create a list of event creators for parsing of Traxis Backend logs.
    :param config: Job configuration.
    :return: A list of event creators.
    """

    timezone_name = config.property("timezone.name")
    timezones_priority = config.property("timezone.priority", "dic")

    general_event_creator = EventCreator(
        Metadata([
            ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
            StringField("level"),
            StringField("message")
        ]),
        RegexpParser(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S+)\s+\[[^\]]+\]\s+([\s\S]*)")
    )

    event_creators = MultipleEventCreator([
        EventCreator(
            Metadata([
                ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                StringField("level"),
                StringField("message"),
                StringField("activity"),
                StringField("requestId")
            ]),
            RegexpParser(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S+)\s+\[[^\]]+\]\s+"
                         r"((OnlineTvaIngest).*\[RequestId\s=\s([^]]+)\][\s\S]*)")
        ),
        EventCreator(
            Metadata([
                ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                StringField("level"),
                StringField("message"),
                StringField("activity"),
                StringField("task"),
                IntField("duration")
            ]),
            RegexpParser(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S+)\s+\[[^\]]+\]\s+"
                         r"((TvaManager).*\[Task\s=\s([^]]+)\].*took\s'(\d+)'\sms[\s\S]*)")
        ),
        EventCreator(
            Metadata([
                ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                StringField("level"),
                StringField("message"),
                StringField("activity"),
                StringField("task"),
                IntField("duration")
            ]),
            RegexpParser(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S+)\s+\[[^\]]+\]\s+((ParsingContext).*"
                         r"\[Task\s=\s([^]]+)\]\sTva\singest\scompleted,\sduration\s=\s(\d+)\sms[\s\S]*)")
        ),
        EventCreator(
            Metadata([
                ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                StringField("level"),
                StringField("message"),
                StringField("activity"),
                StringField("task"),
                IntField("duration")
            ]),
            RegexpParser(r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+(\S+)\s+\[[^\]]+\]\s+((ParsingContext).*"
                         r"\[Task\s=\s([^]]+)\]\sNumber\sof\swrite\sactions\squeued.*took\s(\d+)\sms[\s\S]*)")
        ),
        general_event_creator
    ])

    return MatchField("topic", {
        "traxis_backend_log_gen": MatchField("source", {
            "TraxisService.log": SourceConfiguration(
                event_creators,
                Utils.get_output_topic(config, "general")
            ),
            "TraxisServiceDistributedScheduler.log": SourceConfiguration(
                general_event_creator,
                Utils.get_output_topic(config, "scheduler")
            ),
            "TraxisServiceLogManagement.log": SourceConfiguration(
                general_event_creator,
                Utils.get_output_topic(config, "management")
            )
        }),
        "traxis_backend_log_err": SourceConfiguration(
            general_event_creator,
            Utils.get_output_topic(config, "error")
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
