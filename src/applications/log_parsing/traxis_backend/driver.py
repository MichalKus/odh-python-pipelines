import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
from common.log_parsing.dict_event_creator.event_creator import EventCreator, CompositeEventCreator
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.event_creator_tree.multisource_configuration import *
from common.log_parsing.matchers.matcher import SubstringMatcher
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
            ConfigurableTimestampField("timestamp", timezone_name, timezones_priority, "@timestamp"),
            StringField("level"),
            StringField("message")
        ]),
        RegexpParser(r"^(?P<timestamp>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}.\d+)\s+"
                     r"(?P<level>\S+)\s+\[[^\]]+\]\s+(?P<message>[\s\S]*)")
    )

    tva_ingest_event_creator = EventCreator(
        Metadata([
            StringField("activity"),
            StringField("requestId")
        ]),
        RegexpParser(r"^(?P<activity>OnlineTvaIngest).*\[RequestId\s=\s(?P<requestId>[^]]+)\][\s\S]*",
                     return_empty_dict=True),
        matcher=SubstringMatcher("OnlineTvaIngest")
    )

    tva_manager_event_creator = EventCreator(
        Metadata([
            StringField("activity"),
            StringField("task"),
            IntField("duration")
        ]),
        RegexpParser(r"^(?P<activity>TvaManager).*\[Task\s=\s(?P<task>[^]]+)\].*took\s'(?P<duration>\d+)'\sms[\s\S]*",
                     return_empty_dict=True),
        matcher=SubstringMatcher("TvaManager")
    )

    parsing_context_event_creator = EventCreator(
        Metadata([
            StringField("activity"),
            StringField("task"),
            IntField("duration")
        ]),
        RegexpParser(r"^(?P<activity>ParsingContext).*\[Task\s=\s(?P<task>[^]]+)\]\s"
                     r"Tva\singest\scompleted,\sduration\s=\s(?P<duration>\d+)\sms[\s\S]*",
                     return_empty_dict=True),
        matcher=SubstringMatcher("Tva ingest completed, duration")
    )

    write_actions_event_creator = EventCreator(
        Metadata([
            StringField("activity"),
            StringField("task"),
            IntField("duration")
        ]),
        RegexpParser(r"^(?P<activity>ParsingContext).*\[Task\s=\s(?P<task>[^]]+)\]\s"
                     r"Number\sof\swrite\sactions\squeued.*took\s(?P<duration>\d+)\sms[\s\S]*",
                     return_empty_dict=True),
        matcher=SubstringMatcher("Number of write actions queued")
    )

    return MatchField("topic", {
        "traxis_backend_log_gen": MatchField("source", {
            "TraxisService.log": SourceConfiguration(
                CompositeEventCreator()
                    .add_source_parser(general_event_creator)
                    .add_intermediate_result_parser(tva_ingest_event_creator)
                    .add_intermediate_result_parser(tva_manager_event_creator)
                    .add_intermediate_result_parser(parsing_context_event_creator)
                    .add_intermediate_result_parser(write_actions_event_creator),
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
