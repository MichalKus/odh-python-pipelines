from applications.log_parsing.think_analytics.driver import create_event_creators
from common.log_parsing.log_parsing_processor import LogParsingProcessor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class LogParsingProcessorTestCase(BaseSparkProcessorTestCase):
    def test_log_parsing_processor_success(self):
        self._test_pipeline(
            configuration_path="test/it/resources/log_parsing/configuration.yml",
            processor_creator=self.__create_processor,
            input_dir="test/it/resources/log_parsing//input",
            expected_result_file="test/it/resources/log_parsing/expected_result.txt"
        )

    @staticmethod
    def __create_processor(configuration):
        return LogParsingProcessor(configuration, create_event_creators(configuration))
