"""
Spark driver for parsing STB F5 messages
"""

import sys

from common.kafka_pipeline import KafkaPipeline
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from util.utils import Utils


def create_event_creators(config):
    """
    Method to create a list of event creators
    :param config: Job configuration.
    :return: A list of event creators.
    """

    timezone_name = config.property("timezone.name")
    timezones_priority = config.property("timezone.priority", "dic")

    return None


if __name__ == "__main__":
    configuration = Utils.load_config(sys.argv[:])
    KafkaPipeline(
        configuration,
        LogParsingProcessor(configuration, create_event_creators(configuration))
    ).start()
