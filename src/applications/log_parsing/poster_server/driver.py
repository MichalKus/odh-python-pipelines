import sys

from common.kafka_pipeline import KafkaPipeline
# from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.dict_event_creator.event_creator import CompositeEventCreator, EventCreator
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
# from common.log_parsing.list_event_creator.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration, MatchField
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.metadata import Metadata, StringField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from applications.log_parsing.poster_server.crid_event_creator import CridEventCreator
from util.utils import Utils


def create_event_creators(configuration=None):
    """
    Tree of different parsers for all types of logs for poster server
    :param configuration: YML config
    :return: Tree of event_creators
    """

    timezone_name = configuration.property("timezone.name")
    timezones_property = configuration.property("timezone.priority", "dic")

    poster_server_log = EventCreator(Metadata([ConfigurableTimestampField("timestamp", timezone_name, timezones_property, "@timestamp"),
                                               StringField("level"),
                                               StringField("module"),
                                               StringField("message")]),
                                     RegexpParser(
                                         r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\,\d{3})"
                                         r"\s+(?P<level>\w+?)\s+(?P<module>\w+)\s+(?P<message>.*)"))

    crid_creator = CridEventCreator(Metadata([
        StringField("message")]),
        RegexpParser(r"(?s)^(?P<message>.*)",
                     return_empty_dict=True),
        field_to_parse="message")

    return MatchField("source", {
        "PosterServer.Error.log": SourceConfiguration(
            CompositeEventCreator()
                .add_source_parser(poster_server_log)
                .add_intermediate_result_parser(crid_creator),
            Utils.get_output_topic(configuration, "poster_server_error_log")
        ),
        "PosterServer.log": SourceConfiguration(
            CompositeEventCreator()
                .add_source_parser(poster_server_log)
                .add_intermediate_result_parser(crid_creator),
            Utils.get_output_topic(configuration, "poster_server_log")
        )
    })

if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
