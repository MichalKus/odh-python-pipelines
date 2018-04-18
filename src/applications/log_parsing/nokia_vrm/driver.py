"""Spark driver for parsing message from Nokia_VRM component"""
from common.log_parsing.event_creator_tree.multisource_configuration import MatchField, SourceConfiguration
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.parsers.splitter_parser import SplitterParser
from common.log_parsing.metadata import Metadata, StringField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.kafka_pipeline_helper import start_log_parsing_pipeline
from util.utils import Utils


def create_event_creators(configuration=None):
    """
    Tree of different parsers for all types of logs for Nokia VRM
    :param configuration: YML config
    :return: Tree of event_creators
    """

    timezone_name = configuration.property("timezone.name")
    timezones_property = configuration.property("timezone.priority", "dic")

    nokia_vrm_scheduler_audit_csv = EventCreator(Metadata([
        ConfigurableTimestampField("timestamp", "%d-%b-%Y %H:%M:%S.%f",
                                   timezone_name, timezones_property, "@timestamp"),
        StringField("level"),
        StringField("field1"),
        StringField("event_id"),
        StringField("domain"),
        StringField("ip"),
        StringField("method"),
        StringField("params"),
        StringField("description"),
        StringField("message")]),
        SplitterParser("|", is_trim=True))

    nokia_vrm_audit_csv = EventCreator(Metadata([
        ConfigurableTimestampField("timestamp", "%d-%b-%Y %H:%M:%S.%f",
                                   timezone_name, timezones_property, "@timestamp"),
        StringField("level"),
        StringField("event_id"),
        StringField("domain"),
        StringField("ip"),
        StringField("method"),
        StringField("params"),
        StringField("description"),
        StringField("message")]),
        SplitterParser("|", is_trim=True))

    nokia_vrm_ds_audit_csv = EventCreator(Metadata([
        ConfigurableTimestampField("timestamp", "%d-%b-%Y %H:%M:%S.%f",
                                   timezone_name, timezones_property, "@timestamp"),
        StringField("level"),
        StringField("event_id_1"),
        StringField("event_id_2"),
        StringField("domain"),
        StringField("ip"),
        StringField("method"),
        StringField("params"),
        StringField("description"),
        StringField("message")]),
        SplitterParser("|", is_trim=True))

    nokia_vrm_bs_lgi_lgienh_api_audit_csv = EventCreator(
        Metadata([
            ConfigurableTimestampField("timestamp", "%Y-%m-%d %H:%M:%S.%f",
                                       timezone_name, timezones_property, "@timestamp"),
            StringField("level"),
            StringField("endpoint"),
            StringField("request")]),
        SplitterParser("|", is_trim=True))

    return MatchField("source", {
        "scheduler_bs_audit.log": SourceConfiguration(
            nokia_vrm_scheduler_audit_csv,
            Utils.get_output_topic(configuration, "scheduler_bs_audit")
        ),
        "console_bs_audit.log": SourceConfiguration(
            nokia_vrm_audit_csv,
            Utils.get_output_topic(configuration, "console_bs_audit")
        ),
        "authentication_bs_audit.log": SourceConfiguration(
            nokia_vrm_audit_csv,
            Utils.get_output_topic(configuration, "authentication_bs_audit")
        ),
        "cdvr_bs_audit.log": SourceConfiguration(
            nokia_vrm_audit_csv,
            Utils.get_output_topic(configuration, "cdvr_bs_audit")
        ),
        "epg_audit.log": MatchField("topic", {
            "nokiavrmds_epgaudit": SourceConfiguration(
                nokia_vrm_ds_audit_csv,
                Utils.get_output_topic(configuration, "epg_ds_audit")
            ),
            "nokiavrmbs_epgaudit": SourceConfiguration(
                nokia_vrm_ds_audit_csv,
                Utils.get_output_topic(configuration, "epg_bs_audit")
            )
        }),
        "cDVR_audit.log": SourceConfiguration(
            nokia_vrm_ds_audit_csv,
            Utils.get_output_topic(configuration, "cdvr_ds_audit")
        ),
        "user_audit.log": SourceConfiguration(
            nokia_vrm_ds_audit_csv,
            Utils.get_output_topic(configuration, "user_ds_audit")
        ),
        "lgienhapi_bs.log": SourceConfiguration(
            nokia_vrm_bs_lgi_lgienh_api_audit_csv,
            Utils.get_output_topic(configuration, "lgienh_api_bs_audit")
        ),
        "schange_import_bs_audit.log": SourceConfiguration(
            nokia_vrm_audit_csv,
            Utils.get_output_topic(configuration, "schange_import_bs_audit")
        ),
        "vspp_adapter_bs_audit.log": SourceConfiguration(
            nokia_vrm_audit_csv,
            Utils.get_output_topic(configuration, "vspp_adapter_bs_audit")
        )
    })


if __name__ == "__main__":
    start_log_parsing_pipeline(create_event_creators)
