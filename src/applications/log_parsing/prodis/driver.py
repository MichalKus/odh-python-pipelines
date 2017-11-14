import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.dict_event_creator.event_creator import EventCreator as DictEventCreator
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.multiple_event_creator import MultipleEventCreator
from common.log_parsing.list_event_creator.splitter_parser import SplitterParser
from common.log_parsing.metadata import *
from util.utils import Utils


def create_event_creators(configuration=None):
    """
    Tree of different parsers for all types of logs for PRODIS
    :param configuration: YML config
    :return: Tree of event_creators
    """
    general_regexp_event_creator = DictEventCreator(
        Metadata([
            TimestampField("timestamp", "%Y-%m-%d %H:%M:%S,%f", "@timestamp"),
            StringField("level"),
            StringField("thread"),
            StringField("message")
        ]),
        RegexpParser(
            "^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})(?:\s+)(?P<level>\w+?) \[(?P<thread>\d+)\] - (?P<message>(?:.|\s)*)"
        )
    )

    additional_services_event_event_creator = DictEventCreator(
        Metadata([
            TimestampField("timestamp", "%Y-%m-%d %H:%M:%S,%f", "@timestamp"),
            StringField("level"),
            StringField("thread"),
            StringField("message")
        ]),
        RegexpParser(
            "^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})(?:\s+)(?P<level>\w+?) \[(?P<thread>.*)\] \(:0\) - (?P<message>(?:.|\s)*)"
        ))

    prodis_ws_event_creator_5_columns = EventCreator(
        Metadata([
            TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
            StringField("level"),
            StringField("thread_name"),
            StringField("instance_name"),
            StringField("message")]),
        SplitterParser("|", is_trim=True))

    return MatchField("source", {
        "PRODIS_WS.log": SourceConfiguration(
            MultipleEventCreator([
                prodis_ws_event_creator_5_columns,
                EventCreator(
                    Metadata([
                        TimestampField("@timestamp", "%Y-%m-%d %H:%M:%S,%f"),
                        StringField("level"),
                        StringField("thread_name"),
                        StringField("instance_name"),
                        StringField("component"),
                        StringField("message")
                    ]),
                    SplitterParser("|", is_trim=True)
                )
            ]),
            Utils.get_output_topic(configuration, "ws")
        ),
        "PRODIS_WS.Error.log": SourceConfiguration(
            prodis_ws_event_creator_5_columns, Utils.get_output_topic(configuration, "ws_error")),
        "PRODIS.log": SourceConfiguration(
            general_regexp_event_creator, Utils.get_output_topic(configuration, "prodis")),
        "PRODIS.Error.log": SourceConfiguration(
            general_regexp_event_creator, Utils.get_output_topic(configuration, "prodis_error")),
        "PRODIS_Config.log": SourceConfiguration(
            general_regexp_event_creator, Utils.get_output_topic(configuration, "config")),
        "PRODIS_Config.Error.log": SourceConfiguration(
            general_regexp_event_creator, Utils.get_output_topic(configuration, "config_error")),
        "ProdisRestClients.log": SourceConfiguration(
            additional_services_event_event_creator, Utils.get_output_topic(configuration, "rest_clients")),
        "ProdisRestServices.log": SourceConfiguration(
            additional_services_event_event_creator, Utils.get_output_topic(configuration, "rest_services")),
        "ProdisReportingRestServices.log": SourceConfiguration(
            additional_services_event_event_creator, Utils.get_output_topic(configuration, "reporting_rest_services")),
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
