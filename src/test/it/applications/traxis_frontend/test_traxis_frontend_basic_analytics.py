from applications.basic_analytics.traxis_frontend.general_driver import create_processor as create_general_processor
from applications.basic_analytics.traxis_frontend.error_driver import create_processor as create_error_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class TraxisFrontendBasicAnalyticsTestCase(BaseSparkProcessorTestCase):
    __directory = "test/it/resources/traxis_frontend/"

    def test_general_success(self):
        self._test_pipeline(
            configuration_path=self.__directory + "configuration_general.yml",
            processor_creator=create_general_processor,
            input_dir=self.__directory + "input_general",
            expected_result_file=self.__directory + "expected_result_general.txt"
        )

    def test_error_success(self):
        self._test_pipeline(
            configuration_path=self.__directory + "configuration_error.yml",
            processor_creator=create_error_processor,
            input_dir=self.__directory + "input_error",
            expected_result_file=self.__directory + "expected_result_error.txt"
        )
