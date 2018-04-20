"""Module for counting all general analytics metrics for EOS STB component"""
from util.kafka_pipeline_helper import start_basic_analytics_pipeline


def create_processor(configuration):
    """Method to create the instance of the processor"""


if __name__ == "__main__":
    start_basic_analytics_pipeline(create_processor)
