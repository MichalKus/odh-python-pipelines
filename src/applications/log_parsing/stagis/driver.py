"""Spark driver for parsing message from Stagis component"""
import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.composite_event_creator import CompositeEventCreator
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.multiline_event_creator import MultilineEventCreator
from common.log_parsing.list_event_creator.mutate_event_creator import MutateEventCreator, FieldsMapping
from common.log_parsing.list_event_creator.parsers.regexp_parser import RegexpParser
from common.log_parsing.list_event_creator.parsers.splitter_parser import SplitterParser
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.matchers.matcher import SubstringMatcher
from common.log_parsing.metadata import Metadata, StringField, IntField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util import Utils


def create_event_creators(configuration):
    """
    Create MatchField configuration for Stagis topics that macth patterns:
    - *stagis_log_gen*
    - *stagis_log_err*
    - *stagis_corecommit_log*
    - *stagis_interface_log*
    - *stagis_wcf_log*
    :param configuration: job configuration
    :return: initialized MatchField configuration for Stagis component
    """
    timezone_name = configuration.property("timezone.name")
    timezones_priority = configuration.property("timezone.priority", "dic")

    return MatchField("topic", {
        "stagis_log_gen": SourceConfiguration(
            CompositeEventCreator()
                .add_source_parser(Stagis.ee_event_creator(timezone_name, timezones_priority))
                .add_intermediate_result_parser(Stagis.model_state_event_creator())
                .add_intermediate_result_parser(Stagis.replacer_event_creator(), final=True)
                .add_intermediate_result_parser(Stagis.received_delta_server_notification_event_creator())
                .add_intermediate_result_parser(Stagis.replacer_event_creator(), final=True)
                .add_intermediate_result_parser(Stagis.tva_delta_server_request_event_creator())
                .add_intermediate_result_parser(Stagis.replacer_event_creator(), final=True)
                .add_intermediate_result_parser(Stagis.tva_delta_server_response_event_creator())
                .add_intermediate_result_parser(Stagis.replacer_event_creator(), final=True),
            Utils.get_output_topic(configuration, "general")
        ),
        "stagis_log_err": SourceConfiguration(
            Stagis.ee_event_creator(timezone_name, timezones_priority),
            Utils.get_output_topic(configuration, "error")
        ),
        "stagis_corecommit_log": SourceConfiguration(
            Stagis.ee_corecommit_event_creator(timezone_name, timezones_priority),
            Utils.get_output_topic(configuration, "corecommit")
        ),
        "stagis_interface_log": SourceConfiguration(
            Stagis.ee_interface_event_creator(timezone_name, timezones_priority),
            Utils.get_output_topic(configuration, "interface")
        ),
        "stagis_wcf_log": SourceConfiguration(
            Stagis.ee_wcf_event_creator(timezone_name, timezones_priority),
            Utils.get_output_topic(configuration, "wcf")
        )
    })


class Stagis(object):
    """
    Contains methods for creation EventCreator for specific logs
    """

    @staticmethod
    def _replace_task_name(text):
        dictionary = {
            "TVA Delta Server respond": "TVA Delta Server response",
            "TVA Delta Request Starting": "TVA Delta Server request",
            "Received Delta Server Notification": "Notification",
            "Model state after committing transaction": "Committing Transaction"
        }
        if text in dictionary.keys():
            text = dictionary[text]
        return text

    @staticmethod
    def replacer_event_creator():
        return MutateEventCreator(None, [FieldsMapping(["task"], "task")], Stagis._replace_task_name)

    @staticmethod
    def ee_event_creator(timezone_name, timezones_priority):
        return EventCreator(
            Metadata([
                ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                StringField("level"),
                StringField("instance_name"),
                StringField("causality_id"),
                StringField("thread_id"),
                StringField("class_name"),
                StringField("message")
            ]),
            SplitterParser("|", is_trim=True))

    @staticmethod
    def ee_corecommit_event_creator(timezone_name, timezones_priority):
        return EventCreator(
            Metadata([
                ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                StringField("level"),
                StringField("instance_name"),
                StringField("causality_id"),
                StringField("thread_id"),
                StringField("class_name"),
                StringField("message")
            ]),
            SplitterParser("|", is_trim=True))

    @staticmethod
    def ee_interface_event_creator(timezone_name, timezones_priority):
        return EventCreator(
            Metadata([
                ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                StringField("instance_name"),
                StringField("thread_id"),
                StringField("class_name"),
                StringField("message")
            ]),
            SplitterParser("|", is_trim=True))

    @staticmethod
    def ee_wcf_event_creator(timezone_name, timezones_priority):
        def concat(item1, item2):
            """  method concatenates params """
            return item1 + item2

        def take_first(item1, item2):
            """  method takes left parameter only """
            return item1

        return MultilineEventCreator(
            Metadata([
                ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                StringField("message")
            ]),
            SplitterParser("|", is_trim=True), [take_first, concat])

    @staticmethod
    def tva_delta_server_response_event_creator():
        return EventCreator(
            Metadata([
                StringField("task"),
                StringField("status"),
                IntField("duration")
            ]),
            RegexpParser(
                r"^(?P<task>TVA Delta Server respond) with status code '(?P<status>\S+)' in '(?P<duration>\d+)' ms\s*",
                return_empty_list=True),
            matcher=SubstringMatcher("TVA Delta Server respond"))

    @staticmethod
    def tva_delta_server_request_event_creator():
        return EventCreator(
            Metadata([
                StringField("task"),
                StringField("sequence_number")
            ]),
            RegexpParser(
                r"^(?P<task>TVA Delta Request Starting).+sequence number: (?P<sequence_number>\d+)\s*",
                return_empty_list=True),
            matcher=SubstringMatcher("TVA Delta Request Starting"))

    @staticmethod
    def received_delta_server_notification_event_creator():
        return EventCreator(
            Metadata([
                StringField("task"),
                StringField("sequence_number")
            ]),
            RegexpParser(
                r"^(?P<task>Received Delta Server Notification) Sequence Number: (?P<sequence_number>\d+).*",
                return_empty_list=True),
            matcher=SubstringMatcher("Received Delta Server Notification"))

    @staticmethod
    def model_state_event_creator():
        return EventCreator(
            Metadata([
                StringField("task"),
                StringField("sequence_number"),
                StringField("number"),
                IntField("entities"),
                IntField("links"),
                IntField("channels"),
                IntField("events"),
                IntField("programs"),
                IntField("groups"),
                IntField("on_demand_programs"),
                IntField("broadcast_events"),
            ]),
            RegexpParser(
                r"^\[Model\] (?P<task>Model state after committing transaction) "
                r"\[Sequence number: (?P<sequence_number>\d+).*Number: (?P<number>\d+)\] "
                r"Entities: (?P<entities>\d+) - Links: (?P<links>\d+) - Channels: (?P<channels>\d+)"
                r" - Events: (?P<events>\d+) - Programs: (?P<programs>\d+) - Groups: (?P<groups>\d+)"
                r" - OnDemandPrograms: (?P<OnDemandPrograms>\d+) - BroadcastEvents: (?P<BroadcastEvents>\d+)\s*",
                return_empty_list=True),
            matcher=SubstringMatcher("Model state after committing transaction"))


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
