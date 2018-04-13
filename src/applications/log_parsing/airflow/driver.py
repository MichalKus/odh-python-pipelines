"""Spark driver for parsing message from Airflow component"""
import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.composite_event_creator import CompositeEventCreator
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.dict_event_creator.parsers.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration, MatchField
from common.log_parsing.dict_event_creator.mutate_event_creator import MutateEventCreator, FieldsMapping
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.matchers.matcher import SubstringMatcher
from common.log_parsing.metadata import Metadata, StringField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(configuration):
    """
    Method creates configuration for Airflow Component:
    - worker
    - worker_dag_execution

    :param configuration
    :return: MatchField configuration for Airflow
    """

    timezone_name = configuration.property("timezone.name")
    timezones_property = configuration.property("timezone.priority", "dic")

    general_worker_creator = EventCreator(
        Metadata([
            ConfigurableTimestampField("timestamp", timezone_name, timezones_property, "@timestamp"),
            StringField("script"),
            StringField("level"),
            StringField("message")
        ]),
        RegexpParser(
            r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\] \{(?P<script>[^\}]+)\} (?P<level>\w+?) - "
            r"(?P<message>(.|\s)*)"
        )
    )

    general_manager_creator = EventCreator(
        Metadata([
            ConfigurableTimestampField("timestamp", timezone_name, timezones_property, "@timestamp"),
            StringField("script"),
            StringField("level"),
            StringField("message")
        ]),
        RegexpParser(
            r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\s+"
            r"(?P<message>.*\{(?P<script>[^\}]+)\}.*\s+(?P<level>[a-zA-Z]+) - .*)"
        )
    )

    webui_manager_creator = EventCreator(
        Metadata([
            ConfigurableTimestampField("timestamp", timezone_name, timezones_property, "@timestamp"),
            StringField("message"),
            StringField("message_level")
        ]),
        RegexpParser(
            r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \+\d*)\]\s+"
            r"(?P<message>.*\[(?P<message_level>[a-zA-Z]+)\]\s+.*)",
            return_empty_dict=True
        )
    )

    dag_creator = EventCreator(
        Metadata([
            StringField("dag"),
            StringField("task")
        ]),
        RegexpParser(r".*/usr/local/airflow/logs/(?P<dag>\S+)/(?P<task>[\S|^/]+)/.*",
                     return_empty_dict=True), field_to_parse="source")

    subtask_creator = EventCreator(
        Metadata(
            [ConfigurableTimestampField("subtask_timestamp", timezone_name, timezones_property),
             StringField("subtask_script"),
             StringField("subtask_level"),
             StringField("subtask_message")]),
        RegexpParser(
            r"^Subtask: \[(?P<subtask_timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]"
            r" \{(?P<subtask_script>[^\}]+):\d+\} (?P<subtask_level>\w+?) - (?P<subtask_message>(?:.|\s)*)",
            return_empty_dict=True),
        matcher=SubstringMatcher("Subtask:"),
        field_to_parse="message")

    crid_creator = EventCreator(
        Metadata([
            StringField("crid")
        ]),
        RegexpParser(r"Fabrix input:.*\/(?P<crid>crid[^\/]+)",
                     return_empty_dict=True),
        matcher=SubstringMatcher("Fabrix input:"),
        field_to_parse="subtask_message")

    clean_crid_creator = MutateEventCreator(None, [FieldsMapping(
        ["crid"], "crid", lambda x: x.replace("~~3A", ":").replace("~~2F", "/"))])

    airflow_id_creator = EventCreator(
        Metadata([
            StringField("airflow_id")
        ]),
        RegexpParser(r"Submitting asset:\s+(?P<airflow_id>[\d|\w]{32}_[\d|\w]{32})",
                     return_empty_dict=True),
        matcher=SubstringMatcher("Submitting asset:"),
        field_to_parse="subtask_message")

    clean_script_name_creator = MutateEventCreator(None, [
        FieldsMapping(["script"], "script_name", lambda x: x.split(":")[0], True)
    ])

    message_level_rename_creator = MutateEventCreator(None, [
        FieldsMapping(["level"], "message_level", lambda x: x, True)
    ])

    airflow_manager_dag_creator = EventCreator(
        Metadata([StringField("dag")]),
        RegexpParser(r".*DAG?\(s\).*\['(?P<dag>.*)'\].*"),
        SubstringMatcher("DAG(s)")
    )

    return MatchField("topic", {
        "airflow_worker": MatchField("source", {
            "airflow.log": SourceConfiguration(
                CompositeEventCreator()
                .add_source_parser(general_worker_creator)
                .add_intermediate_result_parser(subtask_creator)
                .add_intermediate_result_parser(crid_creator)
                .add_intermediate_result_parser(clean_crid_creator, final=True)
                .add_intermediate_result_parser(airflow_id_creator, final=True),
                Utils.get_output_topic(configuration, 'worker')
            ),
            "/usr/local/airflow/logs": SourceConfiguration(
                CompositeEventCreator()
                .add_source_parser(general_worker_creator)
                .add_source_parser(dag_creator)
                .add_intermediate_result_parser(subtask_creator)
                .add_intermediate_result_parser(crid_creator)
                .add_intermediate_result_parser(clean_crid_creator, final=True)
                .add_intermediate_result_parser(airflow_id_creator, final=True),
                Utils.get_output_topic(configuration, 'worker_dag_execution')
            )
        }),
        "airflowmanager_scheduler_latest": SourceConfiguration(
            CompositeEventCreator()
            .add_source_parser(general_manager_creator)
            .add_intermediate_result_parser(clean_script_name_creator)
            .add_intermediate_result_parser(airflow_manager_dag_creator)
            .add_intermediate_result_parser(message_level_rename_creator),
            Utils.get_output_topic(configuration, 'manager_scheduler_latest')
        ),
        "airflowmanager_scheduler_airflow": SourceConfiguration(
            CompositeEventCreator()
                .add_source_parser(general_manager_creator)
                .add_intermediate_result_parser(clean_script_name_creator)
                .add_intermediate_result_parser(airflow_manager_dag_creator)
                .add_intermediate_result_parser(message_level_rename_creator),
            Utils.get_output_topic(configuration, 'manager_scheduler_latest')
        ),
        "airflowmanager_webui": SourceConfiguration(
            CompositeEventCreator()
            .add_source_parser(webui_manager_creator, final=True)
            .add_source_parser(general_manager_creator)
            .add_intermediate_result_parser(clean_script_name_creator)
            .add_intermediate_result_parser(airflow_manager_dag_creator)
            .add_intermediate_result_parser(message_level_rename_creator),
            Utils.get_output_topic(configuration, 'manager_webui')
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
