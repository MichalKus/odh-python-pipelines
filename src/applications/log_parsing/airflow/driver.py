"""Spark driver for parsing message from Airflow component"""
import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.composite_event_creator import CompositeEventCreator
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.dict_event_creator.parsers.regexp_parser import RegexpParser
from common.log_parsing.dict_event_creator.predicate_event_creator import PredicateEventCreator
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
    idc_timezones_property = configuration.property("timezone.priority", "idc")

    return MatchField("topic", {
        "airflow_worker": MatchField("source", {
            "airflow.log": SourceConfiguration(
                CompositeEventCreator()
                .add_source_parser(Airflow.general_worker_creator(timezone_name, timezones_property))
                .add_intermediate_result_parser(Airflow.subtask_creator(timezone_name, timezones_property))
                .add_intermediate_result_parser(Airflow.crid_creator())
                .add_intermediate_result_parser(Airflow.clean_crid_creator(), final=True)
                .add_intermediate_result_parser(Airflow.airflow_id_creator(), final=True),
                Utils.get_output_topic(configuration, 'worker')
            ),
            "/usr/local/airflow/logs": SourceConfiguration(
                CompositeEventCreator()
                .add_source_parser(Airflow.general_worker_creator(timezone_name, timezones_property))
                .add_source_parser(Airflow.dags_creator())
                .add_intermediate_result_parser(Airflow.subtask_creator(timezone_name, timezones_property))
                .add_intermediate_result_parser(Airflow.crid_creator())
                .add_intermediate_result_parser(Airflow.clean_crid_creator(), final=True)
                .add_intermediate_result_parser(Airflow.airflow_id_creator(), final=True),
                Utils.get_output_topic(configuration, 'worker_dag_execution')
            )
        }),
        "airflowmanager_scheduler_latest": SourceConfiguration(
            CompositeEventCreator()
            .add_source_parser(Airflow.manager_scheduler_latest_event_creator(timezone_name, timezones_property))
            .add_intermediate_result_parser(Airflow.manager_dag_status_creator())
            .add_intermediate_result_parser(Airflow.manager_dags_creator(), final=True)
            .add_intermediate_result_parser(Airflow.manager_dag_creator(), final=True)
            .add_intermediate_result_parser(Airflow.manager_dag_run_creator(), final=True),
            Utils.get_output_topic(configuration, 'manager_scheduler_latest')
        ),
        "airflowmanager_scheduler_airflow": SourceConfiguration(
            CompositeEventCreator()
            .add_source_parser(Airflow.manager_scheduler_airflow_event_creator(timezone_name, timezones_property))
            .add_intermediate_result_parser(Airflow.manager_dags_creator(), final=True)
            .add_intermediate_result_parser(Airflow.manager_dag_creator(), final=True)
            .add_intermediate_result_parser(Airflow.manager_dag_run_creator(), final=True),
            Utils.get_output_topic(configuration, 'manager_scheduler_airflow')
        ),
        "airflowmanager_webui": SourceConfiguration(
            CompositeEventCreator()
            .add_source_parser(Airflow.webui_manager_creator(timezone_name, idc_timezones_property), final=True)
            .add_source_parser(Airflow.ip_webui_manager_creator(timezone_name, idc_timezones_property), final=True)
            .add_source_parser(Airflow.script_webui_manager_creator(timezone_name, timezones_property), final=True),
            Utils.get_output_topic(configuration, 'manager_webui')
        )
    })


class Airflow(object):
    """
    Contains methods for creation EventCreator for specific logs
    """

    @staticmethod
    def general_worker_creator(timezone_name, timezones_property):
        return EventCreator(
            Metadata([
            ConfigurableTimestampField("timestamp", "%Y-%m-%d %H:%M:%S,%f",
                                       timezone_name, timezones_property, "@timestamp"),
                StringField("script"),
                StringField("level"),
                StringField("message")
            ]),
            RegexpParser(
                r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\] "
                r"\{(?P<script>[^\}]+)\} (?P<level>\w+?) - (?P<message>(.|\s)*)"
            )
        )

    @staticmethod
    def manager_scheduler_latest_event_creator(timezone_name, timezones_property):
        return EventCreator(
            Metadata([
                ConfigurableTimestampField("timestamp", "%Y-%m-%d %H:%M:%S,%f",
                                           timezone_name, timezones_property, "@timestamp"),
                StringField("script"),
                StringField("dag_processor"),
                StringField("script_line"),
                StringField("level"),
                StringField("message")
            ]),
            RegexpParser(
                r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\s+\{(?P<script>.*?):"
                r"(?P<script_line>.*?)\}\s+(?P<dag_processor>.*?)\s+(?P<level>\w+?)\s+-\s+(?P<message>.*)"
            )
        )

    @staticmethod
    def manager_scheduler_airflow_event_creator(timezone_name, timezones_property):
        return EventCreator(
            Metadata([
                ConfigurableTimestampField("timestamp", "%Y-%m-%d %H:%M:%S,%f",
                                           timezone_name, timezones_property, "@timestamp"),
                StringField("script"),
                StringField("script_line"),
                StringField("level"),
                StringField("message")
            ]),
            RegexpParser(
                r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\s+"
                r"\{(?P<script>.*?):(?P<script_line>.*?)\}\s+(?P<level>\w+?)\s+-\s+(?P<message>.*)"
            )
        )

    @staticmethod
    def webui_manager_creator(timezone_name, timezones_property):
        return EventCreator(
            Metadata([
                ConfigurableTimestampField("timestamp", "%Y-%m-%d %H:%M:%S",
                                           timezone_name, timezones_property, "@timestamp",
                                           include_timezone=True),
                StringField("thread_id"),
                StringField("message"),
                StringField("level")
            ]),
            RegexpParser(
                r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\s+?.\d*?)\]\s+?"
                r"\[(?P<thread_id>.*?)\]\s+?\[(?P<level>\w+?)\]\s+?(?P<message>.*)",
                return_empty_dict=True
            )
        )

    @staticmethod
    def script_webui_manager_creator(timezone_name, timezones_property):
        return EventCreator(
            Metadata([
                ConfigurableTimestampField("timestamp", "%Y-%m-%d %H:%M:%S,%f",
                                           timezone_name, timezones_property, "@timestamp"),
                StringField("thread_id"),
                StringField("script"),
                StringField("script_line"),
                StringField("message"),
                StringField("level")
            ]),
            RegexpParser(
                r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]\s+\[(?P<thread_id>.*?)\]\s+"
                r"\{(?P<script>.*?):(?P<script_line>.*?)\}\s+(?P<level>\w+?)\s+-\s+(?P<message>.*)",
                return_empty_dict=False
            )
        )

    @staticmethod
    def ip_webui_manager_creator(timezone_name, timezones_property):
        return EventCreator(
            Metadata([
                ConfigurableTimestampField("timestamp", "%d/%b/%Y:%H:%M:%S",
                                           timezone_name, timezones_property, "@timestamp",
                                           include_timezone=True),
                StringField("message"),
                StringField("ip")
            ]),
            RegexpParser(
                r"^(?P<ip>.*?)\s+-\s+-\s\[(?P<timestamp>\d{2}\/\w+\/\d{4}:\d{2}:\d{2}:\d{2}\s.\d{4})\]\s+"
                r"(?P<message>.*)",
                return_empty_dict=True
            )
        )

    @staticmethod
    def dags_creator():
        return EventCreator(
            Metadata([
                StringField("dag"),
                StringField("task")
            ]),
            RegexpParser(r".*/usr/local/airflow/logs/(?P<dag>\S+)/(?P<task>[\S|^/]+)/.*",
                         return_empty_dict=True), field_to_parse="source"
        )

    @staticmethod
    def subtask_creator(timezone_name, timezones_property):
        return EventCreator(
            Metadata(
            [ConfigurableTimestampField("subtask_timestamp", "%Y-%m-%d %H:%M:%S,%f", timezone_name, timezones_property),
                 StringField("subtask_script"),
                 StringField("subtask_level"),
                 StringField("subtask_message")]),
            RegexpParser(
                r"^Subtask: \[(?P<subtask_timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]"
                r" \{(?P<subtask_script>[^\}]+):\d+\} (?P<subtask_level>\w+?) - (?P<subtask_message>(?:.|\s)*)",
                return_empty_dict=True),
            matcher=SubstringMatcher("Subtask:"),
            field_to_parse="message")

    @staticmethod
    def crid_creator():
        return EventCreator(
            Metadata([
                StringField("crid")
            ]),
            RegexpParser(r"Fabrix input:.*\/(?P<crid>crid[^\/]+)",
                         return_empty_dict=True),
            matcher=SubstringMatcher("Fabrix input:"),
            field_to_parse="subtask_message")

    @staticmethod
    def clean_crid_creator():
        return MutateEventCreator(None, [FieldsMapping(
            ["crid"], "crid", lambda x: x.replace("~~3A", ":").replace("~~2F", "/"))])

    @staticmethod
    def airflow_id_creator():
        return EventCreator(
            Metadata([
                StringField("airflow_id")
            ]),
            RegexpParser(r"Submitting asset:\s+(?P<airflow_id>[\d|\w]{32}_[\d|\w]{32})",
                         return_empty_dict=True),
            matcher=SubstringMatcher("Submitting asset:"),
            field_to_parse="subtask_message")

    @staticmethod
    def manager_dags_creator():
        return EventCreator(
            Metadata([StringField("dag"), StringField("tenant")]),
            RegexpParser(r".*DAG?\(s\).*\['(?P<dag>(?P<tenant>.*?)_.*?)'\].*"),
            SubstringMatcher("DAG(s)")
        )

    @staticmethod
    def manager_dag_creator():
        return EventCreator(
            Metadata([StringField("dag"), StringField("tenant")]),
            RegexpParser(r".*<DAG:\s+(?P<dag>(?P<tenant>.*?)_.*?)>\s+.*"),
            SubstringMatcher("DAG:")
        )

    @staticmethod
    def manager_dag_run_creator():
        return EventCreator(
            Metadata([StringField("dag"), StringField("tenant")]),
            RegexpParser(r".*<DagRun\s+(?P<dag>(?P<tenant>.*?)_.*?)\s+.*"),
            SubstringMatcher("DagRun")
        )

    @staticmethod
    def manager_dag_status_creator():
        return PredicateEventCreator(
            ["message", "message", "message"],
            [(["arking", "failed", "DagRun"], {"status": "FAILURE", "action": "RUN"}),
             (["arking", "success", "DagRun"], {"status": "SUCCESS", "action": "RUN"}),
             (["Created", "DagRun", "Dagrun"], {"action": "RUN"})])


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
