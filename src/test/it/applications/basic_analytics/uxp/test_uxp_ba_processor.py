from applications.basic_analytics.uxp.uxp_driver import create_processor
from test.it.core.base_spark_test_case import BaseSparkProcessorTestCase


class UxpBAProcessorTestCase(BaseSparkProcessorTestCase):
    def test_processing(self):
        self._test_pipeline(
            configuration_path="test/it/resources/basic_analytics/uxp/configuration.yml",
            processor_creator=create_processor,
            input_dir="test/it/resources/basic_analytics/uxp/input",
            expected_result_file="test/it/resources/basic_analytics/uxp/expected_result.txt",
            print_result=False
        )
