from applications.log_parsing.f5.driver import create_event_creators
from common.log_parsing.flume_log_parsing_processor import FlumeLogParsingProcessor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class FlumeLogParsingProcessorTestCase(BaseSparkProcessorTestCase):
    """
    Test case to check FlumeLogParsingProcessor
    """

    def test_log_parsing_processor_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/log_parsing/configuration.yml",
            processor_creator=self.__create_processor,
            input_dir="test/it/resources/log_parsing/flume",
            expected_result_file="test/it/resources/log_parsing/flume_expected_result.txt"
        )

    @staticmethod
    def __create_processor(configuration):
        return FlumeLogParsingProcessor(configuration, create_event_creators(configuration))
