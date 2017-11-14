import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.event_creator import CompositeEventCreator
from common.log_parsing.dict_event_creator.event_creator import EventCreator
from common.log_parsing.dict_event_creator.regexp_parser import RegexpParser
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration, MatchField
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.metadata import Metadata, StringField, TimestampField
from util.utils import Utils


def create_event_creators(configuration=None):
    general_regexp_event_creator = EventCreator(
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

    return MatchField("source", {
        "airflow.log": SourceConfiguration(
            general_regexp_event_creator,
            Utils.get_output_topic(configuration, 'worker')
        ),
        "/usr/local/airflow/logs": SourceConfiguration(
            CompositeEventCreator()
                .add(general_regexp_event_creator)
                .add(
                EventCreator(Metadata([
                    StringField("dag"),
                    StringField("task")
                ]),
                    RegexpParser(".*/usr/local/airflow/logs/(?P<dag>\S+)/(?P<task>[\S|^/]+)/.*",
                                 return_empty_dict=True),
                    field_to_parse="source"
                )),
            Utils.get_output_topic(configuration, 'worker_dag_execution')
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
