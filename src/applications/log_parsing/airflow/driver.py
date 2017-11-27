import sys

from applications.log_parsing.airflow.crid_event_creator import CridEventCreator
from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.event_creator import CompositeEventCreator
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration, MatchField
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.metadata import Metadata, StringField, TimestampField
from util.utils import Utils


def create_event_creators(configuration=None):
    general_creator = EventCreator(
        Metadata([
            TimestampField("timestamp", "%Y-%m-%d %H:%M:%S,%f", "@timestamp"),
            StringField("script"),
            StringField("level"),
            StringField("message")
        ]),
        RegexpParser(
            "^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\] \{(?P<script>[^\}]+)\} (?P<level>\w+?) - (?P<message>(.|\s)*)"
        )
    )

    dag_creator = EventCreator(
        Metadata([
            StringField("dag"),
            StringField("task")
        ]),
        RegexpParser(".*/usr/local/airflow/logs/(?P<dag>\S+)/(?P<task>[\S|^/]+)/.*",
                     return_empty_dict=True), field_to_parse="source")

    subtask_creator = EventCreator(
        Metadata(
            [TimestampField("subtask_timestamp", "%Y-%m-%d %H:%M:%S,%f"),
             StringField("subtask_script"),
             StringField("subtask_level"),
             StringField("subtask_message")]),
        RegexpParser(
            "^Subtask: \[(?P<subtask_timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\] \{(?P<subtask_script>[^\}]+):\d+\} (?P<subtask_level>\w+?) - (?P<subtask_message>(?:.|\s)*)",
            return_empty_dict=True),
        field_to_parse="message")

    crid_creator = CridEventCreator(
        Metadata([
            StringField("crid")
        ]),
        RegexpParser("Fabrix input:.*\/(?P<crid>crid[^\/]+)",
                     return_empty_dict=True),
        field_to_parse="subtask_message")

    airflow_id_creator = EventCreator(
        Metadata([
            StringField("airflow_id")
        ]),
        RegexpParser("Submitting asset:\s+(?P<airflow_id>[\d|\w]{32}_[\d|\w]{32})",
                     return_empty_dict=True),
        field_to_parse="subtask_message")


    return MatchField("source", {
        "airflow.log": SourceConfiguration(
            CompositeEventCreator()
                .add_source_parser(general_creator)
                .add_intermediate_result_parser(subtask_creator)
                .add_intermediate_result_parser(crid_creator)
                .add_intermediate_result_parser(airflow_id_creator),
            Utils.get_output_topic(configuration, 'worker')
        ),
        "/usr/local/airflow/logs": SourceConfiguration(
            CompositeEventCreator()
                .add_source_parser(general_creator)
                .add_source_parser(dag_creator)
                .add_intermediate_result_parser(subtask_creator)
                .add_intermediate_result_parser(crid_creator)
                .add_intermediate_result_parser(airflow_id_creator),
            Utils.get_output_topic(configuration, 'worker_dag_execution')
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
