import sys

from applications.log_parsing.think_analytics.central_parser import CentralParser
from applications.log_parsing.think_analytics.event_ingest_creator import EventIngestCreator
from applications.log_parsing.think_analytics.event_with_url_creator import EventWithUrlCreator
from applications.log_parsing.think_analytics.http_access_parser import HttpAccessParser
from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.dict_event_creator.regexp_matches_parser import RegexpMatchesParser
from common.log_parsing.event_creator_tree.multisource_configuration import SourceConfiguration, MatchField
from common.log_parsing.list_event_creator.event_creator import EventCreator
from common.log_parsing.list_event_creator.regexp_parser import RegexpParser
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from common.log_parsing.metadata import Metadata, StringField
from common.log_parsing.timezone_metadata import ConfigurableTimestampField
from util.utils import Utils


def create_event_creators(configuration):
    """
    Tree of different parsers for all types of logs for THINK ANALYTICS
    :param configuration: YML config
    :return: Tree of event_creators
    """
    timezone_name = configuration.property("timezone.name")
    timezones_priority = configuration.property("timezone.priority", "dic")

    return MatchField("source", {
        "localhost_access_log": SourceConfiguration(
            EventWithUrlCreator(
                Metadata([
                    ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                    StringField("ip"),
                    StringField("thread"),
                    StringField("http_method"),
                    StringField("url"),
                    StringField("http_version"),
                    StringField("response_code"),
                    StringField("response_time")
                ]),
                HttpAccessParser(is_trim=True)
            ),
            Utils.get_output_topic(configuration, "httpaccess")
        ),
        "RE_SystemOut.log": SourceConfiguration(
            EventCreator(
                Metadata([
                    ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                    StringField("level"),
                    StringField("script"),
                    StringField("message")
                ]),
                RegexpParser(
                    "^\[(?P<timestamp>\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\s\D+?)\] (?P<level>\w+?)\s+?-\s+?(?P<script>\S+?)\s+?:\s+?(?P<message>.*)")
            ),
            Utils.get_output_topic(configuration, "resystemout")
        ),
        "REMON_SystemOut.log": SourceConfiguration(
            EventCreator(
                Metadata([
                    ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                    StringField("level"),
                    StringField("script"),
                    StringField("type"),
                    StringField("message")
                ]),
                RegexpParser(
                    "^\[(?P<timestamp>\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}.\d{3}\s\D+?)\] (?P<level>\w+?)\s+?-\s+?(?P<script>\S+?)\s+?:\s+?\[(?P<type>\S+?)\]\s+?-\s+?(?P<message>.*)")
            ),
            Utils.get_output_topic(configuration, "remonsystemout")
        ),
        "Central.log": SourceConfiguration(
            EventCreator(
                Metadata([
                    ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority, dayfirst=True),
                    StringField("level"),
                    StringField("message"),
                    StringField("thread"),
                    StringField("c0"),
                    StringField("c1"),
                    StringField("c2"),
                    StringField("role")
                ]),
                CentralParser(",", '"')
            ),
            Utils.get_output_topic(configuration, "central")
        ),
        "thinkenterprise.log": SourceConfiguration(
            EventCreator(
                Metadata([
                    ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                    StringField("level"),
                    StringField("message")
                ]),
                RegexpParser(
                    "^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}):\s+?(?P<level>\w+?)\s+?-\s+?(?P<message>.*)")
            ),
            Utils.get_output_topic(configuration, "thinkenterprise")
        ),
        "gcollector.log": SourceConfiguration(
            EventCreator(
                Metadata([
                    ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                    StringField("process_uptime"),
                    StringField("message")
                ]),
                RegexpParser(
                    "^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}.\d{4}):\s+?(?P<process_uptime>\d+?\.\d{3}):\s+?(?P<message>.*)")
            ),
            Utils.get_output_topic(configuration, "gcollector")
        ),
        "server.log": SourceConfiguration(
            EventCreator(
                Metadata([
                    ConfigurableTimestampField("@timestamp", timezone_name, timezones_priority),
                    StringField("level"),
                    StringField("class_name"),
                    StringField("thread"),
                    StringField("message")
                ]),
                RegexpParser(
                    "^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+?(?P<level>\w+?)\s+?\[(?P<class_name>.+?)\]\s+?\((?P<thread>.+?)\)\s+?(?P<message>.*)")
            ),
            Utils.get_output_topic(configuration, "server")
        ),
        "RE_Ingest.log": SourceConfiguration(
            EventIngestCreator(
                Metadata([
                    StringField("started_script"),
                    ConfigurableTimestampField("timestamp", timezone_name, timezones_priority, "@timestamp"),
                    StringField("message"),
                    StringField("finished_script"),
                    ConfigurableTimestampField("finished_time", timezone_name, timezones_priority)
                ]),
                RegexpMatchesParser(
                    "Started\s+?(?P<started_script>.*?\.sh)\s+?(?P<timestamp>\w+?\s+?\w+?\s+?\d{1,2}\s+?\d{2}:\d{2}:\d{2}\s+?\w+?\s+?\d{4})(?P<message>(?:.|\s)*)Finished\s+?(?P<finished_script>.*?\.sh)\s+?(?P<finished_time>\w+?\s+?\w+?\s+?\d{1,2}\s+?\d{2}:\d{2}:\d{2}\s+?\w+?\s+?\d{4}).*"
                )
            ),
            Utils.get_output_topic(configuration, "reingest")
        )
    })


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
