import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.multiple_event_creator import MultipleEventCreator
from common.log_parsing.list_event_creator.splitter_parser import SplitterParser
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.metadata import *
from util.utils import Utils


def create_event_creators(configuration=None):
    """
    Tree of different parsers for all types of logs for Nokia VRM
    :param configuration: YML config
    :return: Tree of event_creators
    """

    nokia_vrm_audit_csv = EventCreator(Metadata([TimestampField("@timestamp", "%d-%b-%Y %H:%M:%S.%f"),
                                                 StringField("level"),
                                                 StringField("event_id"),
                                                 StringField("domain"),
                                                 StringField("ip"),
                                                 StringField("method"),
                                                 StringField("params"),
                                                 StringField("description"),
                                                 StringField("message")]),
                                       SplitterParser("|", is_trim=True))

    nokia_vrm_dev_5_columns_csv = EventCreator(Metadata([StringField("level"),
                                                     TimestampField("@timestamp", "%d-%b-%Y %H:%M:%S.%f"),
                                                     StringField("event_id"),
                                                     StringField("thread_name"),
                                                     StringField("message")
                                                     ]),
                                           SplitterParser("|", is_trim=True))

    nokia_vrm_dev_6_columns_csv = EventCreator(Metadata([StringField("level"),
                                               TimestampField("@timestamp", "%d-%b-%Y %H:%M:%S.%f"),
                                               StringField("event_id"),
                                               StringField("thread_name"),
                                               StringField("message"),
                                               StringField("description")
                                               ]),
                                     SplitterParser("|", is_trim=True))
    return MatchField("source", {
        "scheduler_bs_audit.log": SourceConfiguration(
            nokia_vrm_audit_csv,
            Utils.get_output_topic(configuration, "scheduler_bs_audit")
        ),
        "console_bs_audit.log": SourceConfiguration(
            nokia_vrm_audit_csv,
            Utils.get_output_topic(configuration, "console_bs_audit")
        ),
        "scheduler_bs_dev.log": SourceConfiguration(
            MultipleEventCreator([nokia_vrm_dev_5_columns_csv, nokia_vrm_dev_6_columns_csv]),
            Utils.get_output_topic(configuration, "scheduler_bs_dev")
        ),
        "cdvr_bs_dev.log": SourceConfiguration(
            MultipleEventCreator([nokia_vrm_dev_5_columns_csv, nokia_vrm_dev_6_columns_csv]),
            Utils.get_output_topic(configuration, "cdvr_bs_dev")
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
